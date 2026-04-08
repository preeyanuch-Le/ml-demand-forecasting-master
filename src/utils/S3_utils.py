import os
import pickle

import boto3
import tensorflow as tf


def UploadDirToS3(localDIR, s3Path, s3bucket="hal-bi-bucket"):
    """Uploading local folder to S3 by path
    @param localDIR: string, local directory to be uploaded
    @param s3Path: string, S3 path to be uploaded to
    @param s3bucket: string, S3 bucket name
    """
    print("\n[INFO] Uploading results to s3 initiated...")
    print("\n[INFO] Local Source:", localDIR)
    # for all elements in the folder
    os.system("ls -ltR " + localDIR)

    s3 = boto3.resource("s3")
    s3bucket = s3.Bucket(s3bucket)
    print("\n[INFO] S3path: ", s3Path)

    try:
        for path, subdirs, files in os.walk(localDIR):
            for file in files:
                dest_path = path.replace(localDIR, "")
                __s3file = os.path.normpath(s3Path + "/" + dest_path + "/" + file)
                __local_file = os.path.join(path, file)
                print(
                    "\n[INFO] upload : ", __local_file, " to Target: ", __s3file, end=""
                )
                s3bucket.upload_file(__local_file, __s3file)
                print("\n[INFO] ...Success")
    except Exception as e:
        print(" ... Failed!! Quitting Upload!!")
        print(e)
        raise e


def SavePKLS3(pklFile, s3Key, s3bucket="hal-bi-bucket"):
    pickle_byte_obj = pickle.dumps(pklFile)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(s3bucket, s3Key).put(Body=pickle_byte_obj)
    print(f"[INFO] PKL file is saved to: {s3Key}")


def SaveFileS3(file, s3Key, s3bucket="hal-bi-bucket"):
    s3 = boto3.resource("s3")
    s3bucket = s3.Object(s3bucket, s3Key)
    s3bucket.put(Body=file)

    print(f"[INFO] PKL file is saved to: {s3Key}")


def LoadPKLS3(s3Key, s3bucket="hal-bi-bucket"):
    s3_resource = boto3.resource("s3")
    response = s3_resource.Object(s3bucket, s3Key).get()["Body"].read()
    pklFile = pickle.loads(response)
    print(f"[INFO] PKL file is loaded from: {s3Key}")
    return pklFile


def LoadModelS3(s3Key, s3bucket="hal-bi-bucket"):
    s3_resource = boto3.resource("s3")
    response = s3_resource.Object(s3bucket, s3Key).get()["Body"].read()
    model = tf.keras.models.load_model(response)
    print(f"[INFO] Model is loaded from: {s3Key}")
    return model


# response = s3_resource.get_object(Bucket='hal-bi-bucket', Key=fpath)


# localpath = '/home/ec2-user/SageMaker/business-intelligence-notebooks/pricing/pricing_online_refactor/src/script/notebook/save_models/30122021/pricing_v1_30122021/'
# s3_key = 'data_science/pricing/pricing_online_v4/save_model_trial/pricing_v1_30122021/'

# upload_folder_to_s3(localpath, s3_key, s3bucket = "hal-bi-bucket")
