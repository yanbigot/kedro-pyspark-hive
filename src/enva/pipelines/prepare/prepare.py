from kedro.pipeline import Pipeline, node
from enva.nodes.prepare.prepare import (
    create_dummy_df,
    write_to_source_view,
    ensure_prerequisites,
    control_insertion
)


def create_prepare_pipeline():
    return Pipeline(
        [
            node(func=create_dummy_df, inputs=None, outputs="dummy_df", name="create_dummy_df"),
            node(func=ensure_prerequisites, inputs=None, outputs="reqs_ok", name="ensure_prerequisites"),
            node(func=write_to_source_view, inputs="dummy_df", outputs=None, name="write_to_source_view"),
            node(func=control_insertion, inputs=None, outputs="control_ok", name="control_insertion")
        ]
        , tags="pipeline_tag"
    )
