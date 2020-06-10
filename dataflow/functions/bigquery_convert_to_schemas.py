import apache_beam as beam

def bigquery_convert_to_schemas(schema_json):
    schema_converted = beam.io.gcp.bigquery.bigquery.TableSchema()
    for single_field in schema_json['schema']:
        field_schema = beam.io.gcp.bigquery.bigquery.TableFieldSchema()
        if 'fields' in list(single_field.keys()):
            field_schema.name = single_field['name']
            field_schema.type = single_field['type']
            field_schema.mode = single_field['mode']
            single_field_nested = beam.io.gcp.bigquery.bigquery.TableFieldSchema()
            for single_key in list(single_field['fields']):
                single_field_nested.name = single_key['name']
                single_field_nested.type = single_key['type']
                single_field_nested.mode = single_key['mode']
                field_schema.fields.append(single_field_nested)
        else:
            field_schema.name = single_field['name']
            field_schema.type = single_field['type']
            field_schema.mode = single_field['mode']
        schema_converted.fields.append(field_schema)
    return schema_converted