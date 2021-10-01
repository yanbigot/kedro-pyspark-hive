from kedro.pipeline import Pipeline, node


def create_predict_pipeline():
    return Pipeline(
        [
            node(..., name="node1"),
            node(..., name="node2")
        ]
        , tags="pipeline_tag"
    )
