import hvac
import psycopg2
import pandas
import pandas.io.sql as sqlio
import getpass
import subprocess
import os
from sqlalchemy import create_engine
import threading, queue, time, botocore, traceback, logging


class Vault():
    def __init__(self, url):
        self.set_github_token()
        self.client = hvac.Client(url=url)
        self.client.auth.github.login(token=os.environ['GITHUB_TOKEN'])

    def get_secret(self, path, mount_point='kv'):
        return self.client.secrets.kv.read_secret_version(
                                    path=path,
                                    mount_point=mount_point).get('data') \
                                                            .get('data')

    def set_github_token(self):
        if 'GITHUB_TOKEN' not in os.environ.keys():
            print('Insert your Github token to access Vault:')
            github_token = getpass.getpass(prompt='GithubToken')
            os.environ['GITHUB_TOKEN'] = github_token

class Redshift():
    def __init__(self, host, port, dbname, user, password):
        self.conn = psycopg2.connect(
            "host='{}' port={} dbname='{}' user={} password={}" \
            .format(
                host,
                port,
                dbname,
                user,
                password)
        )
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()
        self.sql_alchemy_engine = create_engine(
                f'postgresql://{user}:{password}@{host}:{port}/{dbname}')


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.cur.close()
        self.conn.close()

    def get_pandas_df(self, sql):
        return sqlio.read_sql_query(sql, self.conn)

    def execute(self, sql):
        return self.cur.execute(sql)

    def to_sql(self, df, **args):
        args['con'] = self.sql_alchemy_engine
        args['index'] = False
        return df.to_sql(**args)


class Hal(Redshift):
    def __init__(self):
        hal_secret = Vault(
            url=os.environ['VAULT_URL'],
        ).get_secret(path=os.environ['VAULT_HAL_PATH'])
        super().__init__(
            host=hal_secret.get('host'),
            port=hal_secret.get('port'),
            dbname=hal_secret.get('default_db'),
            user=hal_secret.get('username'),
            password=hal_secret.get('password')
        )

class ParallelProcessingJobs:
    q = queue.Queue()

    def __init__(self, max_concurrent_workers):
        self.max_concurrent_workers = max_concurrent_workers

    def add_job(self, job):
        self.q.put(job)

    def set_worker_threads(self):
        for _ in range(self.max_concurrent_workers):
            threading.Thread(target=self.run_job, daemon=True).start()

    def run_job(self):
        while True:
            try:
                job = self.q.get()
                processor = job['processor']
                run_parameters = job['run_parameters']
                processor.run(**run_parameters)
            except botocore.exceptions.ClientError as client_error:
                if client_error.response['Error']['Code'] == 'ThrottlingException':
                    time.sleep(10)
                    self.q.put(job)
                    self.q.task_done()
                    continue
                else:
                    logging.error(traceback.format_exc())
            except Exception as e:
                logging.error(traceback.format_exc())
            self.results.append(processor.jobs)
            self.q.task_done()

    def print_result(self):
        self.set_results_summary()
        df = pandas.DataFrame(self.results_summary)
        try:
            from IPython.display import display, HTML
            def make_clickable(val):
                return '<a target="_blank" href="{}">{}</a>'.format(val, 'link')
            display(df.style.format({'log': make_clickable}))
        except Exception:
            print(df)

    def set_results_summary(self):
        self.results_summary = list()
        for k in self.results:
            result_raw = k[0].describe()
            result = dict()
            result['name'] = result_raw['ProcessingJobName']
            result['status'] = result_raw['ProcessingJobStatus']
            result['running_time'] = str(result_raw['ProcessingEndTime'] - result_raw['ProcessingStartTime'])
            result['script'] = result_raw['AppSpecification']['ContainerEntrypoint'][-1].split('/')[-1]
            result['log'] = f"https://ap-southeast-1.console.aws.amazon.com/cloudwatch/home?region=ap-southeast-1#logStream:group=/aws/sagemaker/ProcessingJobs;prefix={result['name']}"
            self.results_summary.append(result)

    def run(self):
        self.results = list()
        self.set_worker_threads()
        self.q.join()
        self.print_result()
