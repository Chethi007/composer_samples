import datetime, pandas, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io import BigQuerySource

class Transpose(beam.DoFn):
    def process(self, element):
        
        records = []
        
        location_id = element.get('location_id')
        
        dates = pandas.date_range(start="2020-01-22", end="2020-12-09")
        
        for date in dates:
            sql_date = date.strftime("%Y-%m-%d")
            col_year = date.strftime("%y")
            col_date = str('_{dt.month}_{dt.day}_'.format(dt = date)) + col_year
            #print('col_date: ' + col_date + ", " + "sql_date: " + sql_date)
            
            if col_date in element.keys():
                cases = element.get(col_date)
                record = {'location_id': location_id, 'date': sql_date, 'cases': cases}
                records.append(record)
        
        return records
            
def run():

    PROJECT_ID = 'data-lake-290221' 
    BUCKET = 'gs://dataflow-log-data' 
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    options = PipelineOptions(
    flags=None,
    runner='DirectRunner',
    project=PROJECT_ID,
    job_name='transpose',
    temp_location=BUCKET + '/temp',
    region='us-central1')
    
    options.view_as(SetupOptions).save_main_session = True

    p = beam.pipeline.Pipeline(options=options)

    sql = '''select farm_fingerprint(concat(cast(latitude as string), cast(longitude as string))) as location_id, * from covid19_confirmed.raw_cases'''
    
    #bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)
    bq_source = BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
        
    out_pcoll = query_results | 'Transpose' >> beam.ParDo(Transpose())
    
    #out_pcoll | 'Write to log' >> WriteToText('records.txt')
        
    dataset_id = 'covid19_confirmed'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'daily_cases'
    schema_id = 'location_id:INTEGER,date:DATE,cases:INTEGER'

    out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
         
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()