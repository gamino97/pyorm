from typing import Annotated

from pydantic import BaseModel, Field, create_model


def make_fields_optional[ModelTypeT: type[BaseModel]](
    model_cls: ModelTypeT,
) -> ModelTypeT:
    new_fields = {}

    for f_name, f_info in model_cls.model_fields.items():
        f_dct = f_info.asdict()
        new_fields[f_name] = (
            Annotated[
                f_dct["annotation"] | None,
                *f_dct["metadata"],
                Field(**f_dct["attributes"]),
            ],
            None,
        )

    return create_model(
        f"{model_cls.__name__}Optional",
        __base__=model_cls,
        **new_fields,
    )
