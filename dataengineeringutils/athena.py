from pyathenajdbc import connect

def make_partitions(db_name, table_name, temp_dir):
    """
    Temp dir: e.g 's3://alpha-dag-data-warehouse-template/temp_delete/'
    """

    conn = connect(s3_staging_dir = temp_dir, region_name = 'eu-west-1')

    sql = """
    MSCK REPAIR TABLE {}.{};
    """.format(db_name, table_name)

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()