from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, StreamTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = StreamTableEnvironment.create(exec_env, t_config)

t_env.get_config().get_configuration().set_string("pipeline.classpaths",
        "file:///Users/bgeng/Documents/GitHub/PravegaHackathon/pravega-connectors-flink-1.11_2.12-0.9.1.jar"
        )
        
t_env.execute_sql('''
CREATE TABLE input_table (
  label FLOAT,
  img STRING
) WITH (
  'connector' = 'filesystem', 
  'path' = 'file:///Users/bgeng/Documents/GitHub/PravegaHackathon/ai_flow_workflow/resources/mnist_test_2.csv', 
  'format' = 'csv'
)
''')

t_env.execute_sql('''
CREATE TABLE output_table (
  label FLOAT,
  img STRING
) with (
    'connector' = 'pravega',
    'controller-uri' = 'tcp://localhost:9090',
    'scope' = 'demo-scope',
    'sink.stream' = 'input-stream',
    'format' = 'csv'
)
''')

t_env.from_path('input_table').insert_into('output_table')

t_env.execute("pravega_job")