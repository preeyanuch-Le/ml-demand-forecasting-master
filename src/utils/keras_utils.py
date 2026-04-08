import os

import tensorflow as tf


def KerasLoadModel(path):
    """Loading Tensorflow Keras Model
    @param path: string, file path
    """
    return tf.keras.models.load_model(path)


def SaveTXTFile(results, filePath, fileName):
    fileDir = f"{filePath}/results_{fileName}.txt"

    if not os.path.exists(os.path.dirname(fileDir)):
        os.makedirs(os.path.dirname(fileDir))
    files = open(fileDir, "w")
    files.writelines(results)
    files.close()


def KerasSaveModelLocal(model, filePath, fileName):
    f = f"{filePath}/{fileName}.h5"
    if not os.path.exists(os.path.dirname(f)):
        os.makedirs(os.path.dirname(f))
    model.save(f)
    print(f"model is saved in {f}")
