from pomelo.ml.model.train import train


def test_train() -> None:
    assert train() == 0
