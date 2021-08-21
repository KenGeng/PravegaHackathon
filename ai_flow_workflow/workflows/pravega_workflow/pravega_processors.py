#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os
import pandas as pd
import time
from typing import List
from joblib import dump, load
import numpy as np
import ai_flow as af
from pyflink.table.udf import udf
from pyflink.table import Table, ScalarFunction, DataTypes
from ai_flow.model_center.entity.model_version_stage import ModelVersionStage
from ai_flow.util.path_util import get_file_dir
from ai_flow_plugins.job_plugins.python.python_processor import ExecutionContext, PythonProcessor
from ai_flow_plugins.job_plugins import flink
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score

flink.set_flink_env(flink.FlinkStreamEnv())

IMG_COLUMNS = ['label', 'img']
class DatasetReader(PythonProcessor):

    def process(self, execution_context: ExecutionContext, input_list: List) -> List:
        """
        Read dataset using pandas
        """
        # Gets the registered dataset meta info
        dataset_meta: af.DatasetMeta = execution_context.config.get('dataset')
        # Read the file using numpy
        train_data = pd.read_csv(dataset_meta.uri, header=0)
        # Prepare dataset
        y_train = train_data.pop(IMG_COLUMNS[0])
        return [[train_data[IMG_COLUMNS[1]].values, y_train.values]]


class ModelTrainer(PythonProcessor):

    def process(self, execution_context: ExecutionContext, input_list: List) -> List:
        """
        Train and save KNN model
        """
        model_meta: af.ModelMeta = execution_context.config.get('model_info')
        clf = LogisticRegression(C=50. / 5000, penalty='l1', solver='saga', tol=1)
        x_train, y_train = input_list[0][0], input_list[0][1]
        print(x_train)
        x_train = [[float(j) for j in i.split(' ') ] for i in x_train]
        clf.fit(x_train, y_train)

        model_path = get_file_dir(__file__) + '/saved_model'
        if not os.path.exists(model_path):
            os.makedirs(model_path)
        model_timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
        model_path = model_path + '/' + model_timestamp
        dump(clf, model_path)
        af.register_model_version(model=model_meta, model_path=model_path)
        return []


class ValidateDatasetReader(PythonProcessor):

    def process(self, execution_context: ExecutionContext, input_list: List) -> List:
        """
        Read test dataset
        """
        dataset_meta: af.DatasetMeta = execution_context.config.get('dataset')
        # Read the file using numpy
        test_data = pd.read_csv(dataset_meta.uri, header=0)
        # Prepare dataset
        y_test = test_data.pop(IMG_COLUMNS[0])
        return [[test_data[IMG_COLUMNS[1]].values, y_test.values]]


class ModelValidator(PythonProcessor):

    def __init__(self, artifact):
        super().__init__()
        self.artifact = artifact

    def process(self, execution_context: ExecutionContext, input_list: List) -> List:
        """
        Validate and deploy model if necessary
        """
        current_model_meta: af.ModelMeta = execution_context.config.get('model_info')
        deployed_model_version = af.get_deployed_model_version(model_name=current_model_meta.name)
        new_model_meta = af.get_latest_generated_model_version(current_model_meta.name)
        uri = af.get_artifact_by_name(self.artifact).uri
        if deployed_model_version is None:
            # If there is no deployed model for now, update the current generated model to be deployed.
            af.update_model_version(model_name=current_model_meta.name,
                                    model_version=new_model_meta.version,
                                    current_stage=ModelVersionStage.VALIDATED)
            af.update_model_version(model_name=current_model_meta.name,
                                    model_version=new_model_meta.version,
                                    current_stage=ModelVersionStage.DEPLOYED)
        else:

            x_validate, y_validate = input_list[0][0], input_list[0][1]
            x_validate = [[float(j) for j in i.split(' ')] for i in x_validate]
            clf = load(new_model_meta.model_path)
            scores = cross_val_score(clf, x_validate, y_validate, scoring='precision_macro')[0]
            deployed_clf = load(deployed_model_version.model_path)
            deployed_scores = cross_val_score(deployed_clf, x_validate, y_validate, scoring='precision_macro')[0]


            with open(uri, 'a') as f:
                f.write(
                    'deployed model version: {} scores: {}\n'.format(deployed_model_version.version, deployed_scores))
                f.write('generated model version: {} scores: {}\n'.format(new_model_meta.version, scores))
            if scores >= deployed_scores - 0.5:
                # Deprecate current model and deploy better new model
                af.update_model_version(model_name=current_model_meta.name,
                                        model_version=deployed_model_version.version,
                                        current_stage=ModelVersionStage.DEPRECATED)
                af.update_model_version(model_name=current_model_meta.name,
                                        model_version=new_model_meta.version,
                                        current_stage=ModelVersionStage.VALIDATED)
                af.update_model_version(model_name=current_model_meta.name,
                                        model_version=new_model_meta.version,
                                        current_stage=ModelVersionStage.DEPLOYED)
        return []


class Source(flink.FlinkPythonProcessor):
    def process(self, execution_context: flink.ExecutionContext, input_list: List[Table] = None) -> List[Table]:
        """
        Flink source reader that reads local file
        """
        data_meta = execution_context.config['dataset']
        t_env = execution_context.table_env
        t_env.get_config().get_configuration().set_string("pipeline.classpaths",
            "file:///Users/bgeng/Documents/GitHub/PravegaHackathon/pravega-connectors-flink-1.11_2.12-0.9.1.jar"
            )
        t_env.execute_sql('''
            CREATE TABLE predict_source (
                label FLOAT,
                img STRING
            ) WITH (
                'connector' = 'pravega',
                'controller-uri' = 'tcp://localhost:9090',
                'scope' = 'demo-scope',
                'scan.execution.type' = 'streaming',
                'scan.streams' = 'input-stream',
                'format' = 'csv'
            )
        ''')
        # t_env.execute_sql('''
        # CREATE TABLE predict_source (
        #   label FLOAT,
        #   img STRING
        # ) WITH (
        #   'connector' = 'filesystem',
        #   'path' = 'file:///Users/bgeng/Documents/GitHub/PravegaHackathon/ai_flow_workflow/resources/mnist_test_2.csv',
        #   'format' = 'csv'
        # )
        # ''')
        table = t_env.from_path('predict_source')
        return [table]


class Predictor(flink.FlinkPythonProcessor):
    def __init__(self):
        super().__init__()
        self.model_name = None

    def open(self, execution_context: flink.ExecutionContext):
        self.model_name = execution_context.config['model_info'].name

    def process(self, execution_context: flink.ExecutionContext, input_list: List[Table] = None) -> List[Table]:
        """
        Use pyflink udf to do prediction
        """
        model_meta = af.get_deployed_model_version(self.model_name)
        model_path = model_meta.model_path
        clf = load(model_path)

        # Define the python udf
        class Predict(ScalarFunction):
            def eval(self, label, img):
                records = [[float(j) for j in img.split(' ') ] ]
                # df = pd.DataFrame.from_records(records, columns=['img'])
                return clf.predict(records)[0]

        # Register the udf in flink table env, so we can call it later in SQL statement
        execution_context.table_env.register_function('mypred',
                                                      udf(f=Predict(),
                                                          input_types=[DataTypes.FLOAT(), DataTypes.STRING()],
                                                          result_type=DataTypes.FLOAT()))
        return [input_list[0].select("mypred(label, img)")]


class Sink(flink.FlinkPythonProcessor):

    def process(self, execution_context: flink.ExecutionContext, input_list: List[Table] = None) -> List[Table]:
        """
        Sink Flink Table produced by Predictor to local file
        """
        table_env = execution_context.table_env
        table_env.get_config().get_configuration().set_string("pipeline.operator-chaining",
                                                          "False")
        table_env.execute_sql('''
           CREATE TABLE predict_sink (
               prediction FLOAT 
           ) WITH (
                'connector' = 'pravega',
                'controller-uri' = 'tcp://localhost:9090',
                'scope' = 'demo-scope',
                'sink.stream' = 'output-stream',
                'format' = 'csv'
           )
       ''')
        execution_context.statement_set.add_insert("predict_sink", input_list[0])
