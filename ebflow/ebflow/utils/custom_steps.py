from typing import Union, Dict, Any, Optional

import attrs

import petl
from frictionless import Resource, Step, fields


@attrs.define(kw_only=True, repr=False)
class fill_down(Step):
    type = "fill-down"

    field_names: list[str]

    # Transform

    def transform_resource(self, resource: Resource):
        current = resource.to_copy()

        # Data
        def data():  # type: ignore
            with current:
                column_positions = list(
                    map(lambda col: current.header.index(col), self.field_names)
                )
                yield current.header.to_list()  # type: ignore
                last_row = [None] * len(current.header)
                for row in current.row_stream:  # type: ignore
                    row_list = row.to_list()
                    for i in column_positions:
                        if not row_list[i]:
                            row_list[i] = last_row[i]
                    last_row = row_list
                    yield row_list  # type: ignore

        # Meta
        resource.data = data

    metadata_profile_patch = {
        "required": ["field_names"],
        "properties": {"field_names": {"type": "list"}},
    }


@attrs.define(kw_only=True, repr=False)
class update_jid(Step):
    type = "update-jid"

    field_name: str

    journal_id_field: str

    # Transform

    def transform_resource(self, resource: Resource):
        current = resource.to_copy()

        # Data
        def data():  # type: ignore

            jid_counter = dict()

            with current:
                index = current.header.index(self.field_name)
                jid_field_index = current.header.index(self.journal_id_field)

                yield current.header.to_list()  # type: ignore

                for row in current.row_stream:  # type: ignore
                    row_list = row.to_list()
                    if jid_counter.get(row_list[index]):
                        jid_counter[row_list[index]] += 1
                    else:
                        jid_counter[row_list[index]] = 1

                    row_list[jid_field_index] = jid_counter[row_list[index]]
                    yield row_list  # type: ignore

        # Meta
        resource.data = data

    metadata_profile_patch = {
        "required": ["field_name", "journal_id_field"],
        "properties": {
            "field_name": {"type": "string"},
            "journal_id_field": {"type": "string"},
        },
    }


@attrs.define(kw_only=True, repr=False)
class value_counts(Step):
    """

    Find distinct values for the given field and count the number and relative
    frequency of occurrences. Returns a table mapping values to counts, with
    most common values first.

    """

    type = "value-counts"

    field_names: [str]
    """
    Field names for which the value count is returned. Refer to petl valuecounts function
    https://petl.readthedocs.io/en/stable/_modules/petl/util/counting.html#valuecounts
    """

    # Transform

    def transform_resource(self, resource: Resource):
        table = resource.to_petl()

        fields_required = []
        for field_name in self.field_names:
            fields_required.append(resource.schema.get_field(field_name))

        resource.schema.fields.clear()
        for field in fields_required:
            resource.schema.add_field(field)

        for name in ["count", "frequency"]:
            resource.schema.add_field(fields.AnyField(name=name))

        resource.data = table.valuecounts(*self.field_names)  # type: ignore

    # Metadata

    metadata_profile_patch = {
        "required": ["field_names"],
        "properties": {
            "field_names": {"type": "array"},
        },
    }


@attrs.define(kw_only=True, repr=False)
class custom_table_join(Step):
    """Join tables.

    This step can be added using the `steps` parameter
    for the `transform` function.

    """

    type = "table-join"

    resource: Union[Resource, str]
    """
    Resource with which to apply join.
    """

    field_names: [str] = []
    """
    Field names with which the join will be performed comparing it's value between two tables.
    If not provided natural join is tried. For more information, please see the following document:
    https://petl.readthedocs.io/en/stable/_modules/petl/transform/joins.html
    """

    left_fields: [str] = []

    right_fields: [str] = []
    """
    left_fields and right_fields are field names on the left and right table respectively on which the join is performed
    """

    use_hash: bool = False
    """
    Specify whether to use hash or not. If True, an alternative implementation of join will be used.
    """

    mode: str = "inner"
    """
    Specifies which mode to use. The available modes are: "inner", "left", "right", "outer", "cross" and
    "negate". The default mode is "inner".
    """

    # Transform

    def transform_resource(self, resource: Resource):
        target = resource
        source = self.resource
        if isinstance(source, str):
            assert target.package
            source = target.package.get_resource(source)
        source.infer()  # type: ignore
        view1 = target.to_petl()  # type: ignore
        view2 = source.to_petl()  # type: ignore
        print(target.header, "\n", source.header)

        if self.field_names:
            self.left_fields = self.field_names
            self.right_fields = self.field_names

        if self.mode not in ["negate"]:
            for field in source.schema.fields:  # type: ignore
                if field.name not in self.right_fields:
                    target.schema.fields.append(field.to_copy())
        if self.mode == "inner":
            join = petl.hashjoin if self.use_hash else petl.join  # type: ignore
            resource.data = join(view1, view2, lkey=self.left_fields, rkey=self.right_fields)  # type: ignore
        elif self.mode == "left":
            leftjoin = (  # type: ignore
                petl.hashleftjoin if self.use_hash else petl.leftjoin  # type: ignore
            )
            resource.data = leftjoin(view1, view2, lkey=self.left_fields, rkey=self.right_fields)  # type: ignore
        elif self.mode == "right":
            rightjoin = (  # type: ignore
                petl.hashrightjoin if self.use_hash else petl.rightjoin  # type: ignore
            )
            resource.data = rightjoin(view1, view2, lkey=self.left_fields, rkey=self.right_fields)  # type: ignore
        elif self.mode == "outer":
            resource.data = petl.outerjoin(view1, view2, lkey=self.left_fields, rkey=self.right_fields)  # type: ignore
        elif self.mode == "cross":
            resource.data = petl.crossjoin(view1, view2)  # type: ignore
        elif self.mode == "negate":
            antijoin = (  # type: ignore
                petl.hashantijoin if self.use_hash else petl.antijoin  # type: ignore
            )
            resource.data = antijoin(view1, view2, lkey=self.left_fields, rkey=self.right_fields)  # type: ignore

    # Metadata

    metadata_profile_patch = {
        "required": ["resource"],
        "properties": {
            "resource": {"type": ["object", "string"]},
            "fieldName": {"type": "string"},
            "mode": {
                "type": "string",
                "enum": ["inner", "left", "right", "outer", "cross", "negate"],
            },
            "hash": {},
        },
    }
    
    
@attrs.define(kw_only=True, repr=False)
class custom_row_compare(Step):
    """Join tables.

    This step can be added using the `steps` parameter
    for the `transform` function.

    """

    type = "table-join"

    resource: Union[Resource, str]
    """
    Resource with which to apply join.
    """

    field_names: [str] = []
    """
    Field names with which the join will be performed comparing it's value between two tables.
    If not provided natural join is tried. For more information, please see the following document:
    https://petl.readthedocs.io/en/stable/_modules/petl/transform/joins.html
    """

    left_fields: [str] = []

    right_fields: [str] = []
    """
    left_fields and right_fields are field names on the left and right table respectively on which the join is performed
    """

    use_hash: bool = False
    """
    Specify whether to use hash or not. If True, an alternative implementation of join will be used.
    """

    mode: str = "inner"
    """
    Specifies which mode to use. The available modes are: "inner", "left", "right", "outer", "cross" and
    "negate". The default mode is "inner".
    """

    # Transform

    def transform_resource(self, resource: Resource):
        target = resource
        source = self.resource
        if isinstance(source, str):
            assert target.package
            source = target.package.get_resource(source)
        source.infer()  # type: ignore
        view1 = target.to_petl()  # type: ignore
        view2 = source.to_petl()  # type: ignore
        print(target.header, "\n", source.header)

        if self.field_names:
            self.left_fields = self.field_names
            self.right_fields = self.field_names
        
        target.schema.add_field(fields.AnyField(name="overlap"))
        
        for i in view2[1:]:
            print("i-----> ", i)
        
        table1_res= []
        for row in view1[1:]:
            print("row-----> ", row)
            if tuple(row) in view2:
                table1_res.append("Y")
            else:
                table1_res.append("N")
        x = petl.addcolumn(view1, 'overlap', table1_res)
        resource.data = x

@attrs.define(kw_only=True, repr=False)
class custom_aggregate(Step):
    """Aggregate table.

    This step can be added using the `steps` parameter
    for the `transform` function.

    """

    type = "table-aggregate"

    aggregation: Dict[str, Any]
    """
    A dictionary with aggregation function. The values
    could be max, min, len and sum.
    """

    group_name: Optional[str] = None
    """
    Field by which the rows will be grouped.
    """

    # Transform

    def transform_resource(self, resource: Resource):
        table = resource.to_petl()  # type: ignore
        field = resource.schema.get_field(self.group_name) if self.group_name else None
        resource.schema.fields.clear()
        if field:
            resource.schema.add_field(field)
        for name in self.aggregation.keys():
            resource.schema.add_field(fields.AnyField(name=name))
        resource.data = table.aggregate(self.group_name, self.aggregation)  # type: ignore

    # Metadata

    metadata_profile_patch = {
        "type": "object",
        "required": ["aggregation"],
        "properties": {
            "groupName": {"type": "string"},
            "aggregation": {"type": "object"},
        },
    }


def custom_sum(values):
    total = 0
    for val in values:
        total += val or 0

    return total


def check_field_gaps(values):
    gap = 0
    for val in values:
        if not val:
            gap += 1
    return gap


def jid_details(values):
    """
    considering the 1st row to always be in sequence or the percentage will never be 100.
    missing_seq_count only include cases where the current row is numeric and prev row is not in sequence
    null_seq_count only includes cases where current row is None
    """
    journal_id = None
    journal_id_prev = None
    null_seq_count = 0
    missing_seq_count = 0
    non_numeric_journal_id = False

    for val in values:
        if not val:
            null_seq_count += 1
        else:
            try:
                journal_id = float(val)
            except ValueError:
                non_numeric_journal_id = True
                break
            if journal_id_prev and journal_id - journal_id_prev != 1:
                missing_seq_count += 1
        journal_id_prev = journal_id

    return non_numeric_journal_id, missing_seq_count, null_seq_count
