from typing import List, Optional, Union

from koheesio.models import BaseModel, Field, InstanceOf
from koheesio.spark import Column
from koheesio.spark.readers import Reader
from koheesio.spark.transformations import Transformation
from koheesio.spark.transformations.lookup import JoinHint, JoinType
from koheesio.spark.utils.common import DataFrame
from pyspark.sql.functions import col

LEFT_ALIAS = "left"
RIGHT_ALIAS = "right"


class JoinDescription(BaseModel):
    how: JoinType = Field(
        description="What type of join to perform." + JoinType.__doc__,
    )
    on: Union[str, List[str], Column, List[Column]] = Field(
        description="Join condition",
    )

    hint: Optional[JoinHint] = Field(
        default=None,
        description="What type of join hint to use. Defaults to None. " + JoinHint.__doc__,
    )

    transformations: List[InstanceOf[Transformation]] = Field(
        default=[],
        description="Transformations that need to be applied 'right' dataframe *before* join",
    )

    # FIXME: witch back to ListOfColumns once Koheesio fixes the boolean conversion bug. See https://github.com/Nike-Inc/koheesio/issues/222
    select: Optional[List[Union[str, Column]]] = Field(
        default=None,
        description="Column names that should be extracted from the 'right' dataframe to the result dataframe. "
        "If not specified, all columns will be used.",
    )


class JoinTransformation(Transformation):
    """
    Join transformation for joining two dataframes together

    Note: pylint disable comments are needed because static analysis tools
    cannot resolve Pydantic model attributes that are created dynamically at runtime.
    """

    reader: InstanceOf[Reader] = Field(description="Reader to get 'right' df to join")
    join_description: JoinDescription = Field(description="Describe how we need to join two dataframes")

    @staticmethod
    def left_alias() -> str:
        return LEFT_ALIAS

    @staticmethod
    def right_alias() -> str:
        return RIGHT_ALIAS

    def execute(self):
        right_df = self._prepare_right()
        joined_df = self.df.alias(LEFT_ALIAS).join(
            right_df.alias(RIGHT_ALIAS),
            on=self.join_description.on,  # pylint: disable=no-member
            how=self.join_description.how,  # pylint: disable=no-member
        )
        self.output.df = joined_df.select(*self._select_columns(self.df, right_df))

    def _prepare_right(self) -> DataFrame:
        right_df = self.reader.read()  # pylint: disable=no-member
        for transformation in self.join_description.transformations:  # pylint: disable=no-member
            right_df = transformation.transform(right_df)
        if self.join_description.hint:  # pylint: disable=no-member
            right_df = right_df.hint(self.join_description.hint)  # pylint: disable=no-member
        return right_df

    def _select_columns(self, df: DataFrame, right_df: DataFrame) -> List[Union[str, Column]]:
        left_columns_to_select = [col(f"{LEFT_ALIAS}.{column}") for column in df.columns]
        additional_columns_to_select = (
            self.join_description.select  # pylint: disable=no-member
            if self.join_description.select  # pylint: disable=no-member
            else right_df.columns
        )

        return left_columns_to_select + list(additional_columns_to_select)
