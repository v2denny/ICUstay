import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
import datetime

# Process elements from the input data.
class PrepareData(beam.DoFn):
    def process(self, element):
        return [(element['ICUSTAY_ID'], (element['ITEMID'], element['VALUE'], element['CHARTTIME'], element['LOS']))]

# Consolidate measures by integer timestamp
class ConsolidateMeasures(beam.DoFn):
    def process(self, element):
        icustay_id, measures = element
        consolidated = {}
        
        # Ensure the first CHARTTIME is a string before converting to datetime
        start_time = measures[0][2]
        if isinstance(start_time, str):
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S%z")
        
        for itemid, value, charttime, los in measures:
            # Ensure CHARTTIME is a datetime object before calculating time difference
            if isinstance(charttime, str):
                charttime = datetime.datetime.strptime(charttime, "%Y-%m-%d %H:%M:%S%z")
            time_diff = int((charttime - start_time).total_seconds() / 60)  # Minutes since start_time
            if time_diff not in consolidated:
                consolidated[time_diff] = []
            consolidated[time_diff].append((itemid, value))
        yield (icustay_id, consolidated, los)

# Format the output
class FormatOutput(beam.DoFn):
    def process(self, element):
        icustay_id, consolidated, los = element
        sequences = []
        for time_diff, measures in consolidated.items():
            measures_str = ",".join([f"({itemid},{value})" for itemid, value in measures])
            sequences.append(f"[{time_diff},{measures_str}]")
        padded_sequence = "[" + ",".join(sequences) + "]"
        yield f'{icustay_id},"{padded_sequence}",{los}'

# Padding
class PadSequences(beam.DoFn):
    def process(self, element, max_length=143210):
        icustay_id, padded_sequence, los = element
        if len(padded_sequence) > max_length:
            padded_sequence = padded_sequence[:max_length]
        yield (icustay_id, padded_sequence, los)

# Pipeline options
options = PipelineOptions(
    project='cdla-trabalho',
    runner='DataflowRunner',
    region='us-central1',
    staging_location='gs://events_trabalho_cdle/staging',
    temp_location='gs://events_trabalho_cdle/temp',
    save_main_session=True
)

# Query for training data
training_query = """
SELECT *
FROM `CHARTEVENTS.training_data`
"""

# Query for test data
test_query = """
SELECT *
FROM `CHARTEVENTS.testing_data`
"""

# Function to create pipeline
def run_pipeline(query, output_prefix):
    with beam.Pipeline(options=options) as p:
        header = 'ICUSTAY_ID,Padded_Sequence,LOS'
        
        (p
         | 'Read from BigQuery' >> ReadFromBigQuery(query=query, use_standard_sql=True)
         | 'PrepareData' >> beam.ParDo(PrepareData())
         | 'FormatData' >> beam.Map(lambda element: (element[0], [element[1]]))
         | 'CombinePerKey' >> beam.CombinePerKey(lambda values: sum(values, []))
         | 'SortValues' >> beam.Map(lambda kv: (kv[0], sorted(kv[1], key=lambda x: x[2])))  # Sort values by CHARTTIME
         | 'ConsolidateMeasures' >> beam.ParDo(ConsolidateMeasures())
         | 'PadSequences' >> beam.ParDo(PadSequences())
         | 'FormatOutput' >> beam.ParDo(FormatOutput())
         | 'Write to Text' >> beam.io.WriteToText(f'gs://events_trabalho_cdle/{output_prefix}', file_name_suffix='.csv', header=header, shard_name_template=''))

# Run pipeline for training data
run_pipeline(training_query, 'training')

# Run pipeline for test data
run_pipeline(test_query, 'testing')
