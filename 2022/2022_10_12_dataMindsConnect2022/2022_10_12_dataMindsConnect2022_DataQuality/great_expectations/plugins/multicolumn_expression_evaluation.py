from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.util import (
    num_to_str,
    substitute_none_for_missing,
    parse_row_condition_string_pandas_engine,
)
from scipy import stats as stats
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.execution_engine.sparkdf_execution_engine import F
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


class MulticolumnExpressionTrue(MulticolumnMapMetricProvider):
    """MetricProvider Class for Custom Expression Evaluation by Dave Ruijter"""

    condition_metric_name = "multicolumn_expression.true"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = "e"

    # @multicolumn_condition_partial(engine=PandasExecutionEngine)
    # def _pandas(cls, column_list, **kwargs):
    #     row_wise_cond = True == True
    #     return row_wise_cond

    # @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_list, **kwargs):
    #     row_wise_cond = True == True
    #     return row_wise_cond

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        expr = kwargs.get("e")
        row_wise_cond = F.expr(expr) == True
        return row_wise_cond


class ExpectMulticolumnExpressionToEvaluateTrue(MulticolumnMapExpectation):
    """
    This is a multi-column expectation, meaning it will be evaluated row-by-row.
    It expects that the given expression evaluates to true for every row.
    You can use any SparkSQL expression, for example: 
    ((columnB IN ('xyz') and columnA IN ('abc')) or (columnB NOT IN ('xyz')))
    
    In the validation / Data Docs page this expectation will be displayed in the 'table' expectations section.

    Note: this expectation is currently only implemented for the the Spark dataframe engine (not for Pandas and SQL Alchemy).
    Args:
        column_list (tuple or list): Set of columns to be used in the expression. Tip: include the identifier columns, as these columns will also be displayed in the unexpected_table, and adding them will help troubleshoot the unexpected values.
        e (string): Expression to be evaluated. NB: in the future this will be called "expression" (a bug prevents me from doing that right now).
        expression_description (string)": Description for the expression. A user-friendly statement to explain what the expression evaluates.

    Keyword Args:
        ignore_row_if (str): "all_values_are_missing", "any_value_is_missing", "never"

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification.

    Returns:
        An ExpectationSuiteValidationResult
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",
        "package": "great_expectations_experimental",
        "tags": ["experimental"],
        "contributors": [
            "@DaveRuijter",
        ],
        "requirements": [],
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    map_metric = "multicolumn_expression.true"
    success_keys = ("e", "expression_description", "mostly")

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "ignore_row_if": "all_values_are_missing",
        "result_format": "SUMMARY",  # Attention: COMPLETE will return **all** the unexpected records, and thus can be VERY large output :)
        "include_config": True,
        "catch_exceptions": True,
    }

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")

        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column_list",
                "ignore_row_if",
                "row_condition",
                "condition_parser",
                "mostly",
            ],
        )
        params_with_json_schema = {
            "column_list": {
                "schema": {"type": "array"},
                "value": params.get("column_list"),
            },
            "ignore_row_if": {
                "schema": {"type": "string"},
                "value": params.get("ignore_row_if"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
            "mostly": {
                "schema": {"type": "number"},
                "value": params.get("mostly"),
            },
            "mostly_pct": {
                "schema": {"type": "number"},
                "value": params.get("mostly_pct"),
            },
        }

        if params["mostly"] is not None:
            params_with_json_schema["mostly_pct"]["value"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
        mostly_str = (
            ""
            if params.get("mostly") is None
            else ", at least $mostly_pct % of the time"
        )

        expression_description_str = params.get("expression_description")
        template_str = f"{expression_description_str}<br><br>Expression: $e{mostly_str}.<br><br>Given input columns: "
        column_list = params.get("column_list") if params.get("column_list") else []

        if len(column_list) > 0:
            for idx, val in enumerate(column_list[:-1]):
                param = f"$column_list_{idx}"
                template_str += f"{param}, "
                params[param] = val

            last_idx = len(column_list) - 1
            last_param = f"$column_list_{last_idx}"
            template_str += last_param
            params[last_param] = column_list[last_idx]

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params_with_json_schema.update(conditional_params)

        return (template_str, params_with_json_schema, styling)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")

        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column_list",
                "ignore_row_if",
                "row_condition",
                "condition_parser",
                "mostly",
            ],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
        mostly_str = (
            ""
            if params.get("mostly") is None
            else ", at least $mostly_pct % of the time"
        )

        expression_description_str = params.get("expression_description")
        template_str = f"{expression_description_str}<br><br>Expression: $e{mostly_str}.<br><br>Given input columns: "
        for idx in range(len(params["column_list"]) - 1):
            template_str += "$column_list_" + str(idx) + ", "
            params["column_list_" + str(idx)] = params["column_list"][idx]

        last_idx = len(params["column_list"]) - 1
        template_str += "$column_list_" + str(last_idx)
        params["column_list_" + str(last_idx)] = params["column_list"][last_idx]

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        result_dict = result.result
        if result_dict is None:
            return "---"

        if result_dict.get("observed_value"):
            observed_value = result_dict.get("observed_value")
            if isinstance(observed_value, (int, float)) and not isinstance(
                observed_value, bool
            ):
                return num_to_str(observed_value, precision=10, use_locale=True)
            return str(observed_value)
        elif result_dict.get("unexpected_percent") is not None:
            return (
                num_to_str(result_dict.get("unexpected_percent"), precision=5)
                + "% unexpected"
            )
        else:
            return "--"

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.unexpected_table")
    def _diagnostic_unexpected_table_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        try:
            result_dict = result.result
        except KeyError:
            return None

        if result_dict is None:
            return None

        if not result_dict.get("partial_unexpected_list") and not result_dict.get(
            "partial_unexpected_counts"
        ):
            return None

        table_rows = []

        if (
            result_dict.get("partial_unexpected_counts")
            and not result_dict.get("partial_unexpected_counts")[0]
            == "partial_exception_counts requires a hashable type"
        ):
            # We will check to see whether we have *all* of the unexpected values
            # accounted for in our count, and include counts if we do. If we do not,
            # we will use this as simply a better (non-repeating) source of
            # "sampled" unexpected values
            total_count = 0
            for unexpected_count_dict in result_dict.get("partial_unexpected_counts"):
                value = unexpected_count_dict.get("value")
                count = unexpected_count_dict.get("count")
                total_count += count
                if value is not None and value != "":
                    table_rows.append([value, count])
                elif value == "":
                    table_rows.append(["EMPTY", count])
                else:
                    table_rows.append(["null", count])

            # Check to see if we have *all* of the unexpected values accounted for. If so,
            # we show counts. If not, we only show "sampled" unexpected values.
            if total_count == result_dict.get("unexpected_count"):
                header_row = ["Unexpected Value", "Count"]
            else:
                header_row = ["Sampled Unexpected Values"]
                table_rows = [[row[0]] for row in table_rows]
        else:
            header_row = []
            partial_unexpected_list = result_dict.get("partial_unexpected_list")
            for unexpected_value_list_item in partial_unexpected_list:
                if unexpected_value_list_item:
                    table_rows.append(list(unexpected_value_list_item.values()))
                if not header_row:
                    header_row = list(unexpected_value_list_item.keys())

        unexpected_table_content_block = RenderedTableContent(
            **{
                "content_block_type": "table",
                "table": table_rows,
                "header_row": header_row,
                "styling": {
                    "body": {"classes": ["table-bordered", "table-sm", "mt-3"]}
                },
            }
        )

        return unexpected_table_content_block
