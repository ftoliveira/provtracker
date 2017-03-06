import os
import uuid

from vistrails.core.modules.vistrails_module import Module, Streaming

import time

ssh = None
chan = None

i = 0


def check_count():
    count = os.getenv('MODULE_COUNT', None)
    if count:
        count_num = int(count)
        count_num += 1
        os.environ['MODULE_COUNT'] = str(count_num)
    else:
        count = 1
        os.environ['MODULE_COUNT'] = str(count)

    return os.environ['MODULE_COUNT']


def send_and_wait_cmd(cmd):
    print('NOVO Comando: ' + cmd)
    chan.send(cmd)
    buff = ''
    while buff.find('>>>') < 0:
        resp = chan.recv(99999)
        buff += resp
        
    print buff
    return buff


def is_sequence(arg):
    return (not hasattr(arg, "strip") and hasattr(arg, "__getitem__") or hasattr(arg, "__iter__"))


def check_script_file():
    script_file = os.getenv('SCRIPT_FILE', None)
    if script_file:
        out_script = open(script_file, 'a')
    else:
        script_file = 'E:\\BOTNET\\SCRIPTS_ALTERADOS\\{0}.py'.format(str(uuid.uuid4()).replace('-', ''), )
        os.environ['SCRIPT_FILE'] = script_file
        out_script = open(script_file, 'w')
    return out_script


def generate_uuid():
    return 'a' + str(uuid.uuid4()).replace('-', '')


class PySparkShell(Module):
    _input_ports = [
        ('host', '(basic:String)', {'optional': False, 'defaults': "['192.168.90.3']"}),
        ('username', '(basic:String)', {'optional': False, 'defaults': "['tosta']"}),
        ('password', '(basic:String)', {'optional': False, 'defaults': "['tosta123']"}),
        ('master', '(basic:String)', {'optional': False, 'defaults': "['192.168.90.3']"}),
        ('master_port', '(basic:String)', {'optional': False, 'defaults': "['7077']"}),
        ('driver_memory', '(basic:String)', {'optional': False, 'defaults': "['32G']"}),
        ('executor_memory', '(basic:String)', {'optional': False, 'defaults': "['128G']"}),
        ('total_executor_cores', '(basic:String)', {'optional': False, 'defaults': "['320']"}),
    ]

    _output_ports = [
        ('pyspark_shell', '(basic:Module)'),
    ]

    def compute(self):
        #--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/home/tosta/logs
    
        import paramiko
        global ssh
        global chan

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.get_input('host'), username=self.get_input('username'), password=self.get_input('password'))
        chan = ssh.invoke_shell()
        chan.settimeout(None)

        print 'Conectando ao pyspark...'
        send_and_wait_cmd('./spark-2.0.0-bin-hadoop2.7/bin/pyspark --driver-memory {0} --executor-memory {1} --total-executor-cores {2} --master spark://{3}:{4}\n'\
        .format(self.get_input('driver_memory'), self.get_input('executor_memory'), self.get_input('total_executor_cores'), self.get_input('master'), self.get_input('master_port')))

        #send_and_wait_cmd('sc.applicationId\n')

        self.set_output('output', self)


class LoadNetworkFlows(Module):
    _input_ports = [
        ('netflow_file', '(basic:String)', {'optional': False, 'defaults': "['/home/tosta/capture20110816.binetflow']"}),
        ('header', '(basic:Boolean)', {'optional': True, 'defaults': "['True']"}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('dataframe', '(basic:String)')
    ]

    def compute(self):
        cmds = []
        
        cmds.append('from pyspark.ml.feature import StringIndexer\n')
        cmds.append('from pyspark.ml import Pipeline\n')
        cmds.append('from pyspark.ml.pipeline import PipelineModel\n')
        
        uid = generate_uuid()
        
        cmds.append(uid + " = spark.read.csv('{0}', header={1})\n".format(self.get_input('netflow_file'), self.get_input('header')))

        for command in cmds:
            send_and_wait_cmd(command)

        self.set_output('output', self)
        self.set_output('dataframe', uid)
        
        


# COLOCAR A OPCAO: NO LABEL! Pra indicar que queremos apenas descobrir o label novo
class SelectFeaturesAndLabel(Module):
    _input_ports = [
        ('dataframe', '(basic:String)', {'optional': False}),
        ('features', '(basic:String)', {'optional': False, 'defaults': "['Dur, Proto, Dir, State, sTos, dTos, TotPkts, TotBytes, SrcBytes']"}),
        ('discrete_features', '(basic:String)', {'optional': False, 'defaults': "['Proto, Dir, State']"}),
        ('handle_invalid', '(basic:String)', {'optional': False, 'defaults': '["skip"]'}),
        ('label_col', '(basic:String)', {'optional': False, 'defaults': "['label']"}),
        ('label_bot', '(basic:String)', {'optional': False, 'defaults': "['Bot']"}),
        ('label_normal', '(basic:String)', {'optional': False, 'defaults': "['Normal']"}),
        ('label_background', '(basic:String)', {'optional': False, 'defaults': "['Background']"}),
        ('save_model', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('save_model_dir', '(basic:String)', {'optional': True, 'defaults': "['hdfs://192.168.90.3:9000/user/tosta_hdfs/']"}),
        ('load_model', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('load_model_file', '(basic:String)', {'optional': True, 'defaults': "['hdfs://192.168.90.3:9000/user/tosta_hdfs/model_XPTO']"}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('dataframe', '(basic:String)')
    ]

    def compute(self):
        cmds = []
        
        cmds.append('from pyspark.ml.linalg import Vectors\n')
        cmds.append('from pyspark.ml.feature import StringIndexer\n')
        cmds.append('from pyspark.sql.functions import regexp_replace\n')
        
        
        
        cmds.append("""
def sparsefy(r, featuresLen):    
    vectorIndexes = []
    values = []
    for idx in range(featuresLen):
        if r[idx + 3] == 0 or r[idx + 3]:
            vectorIndexes.append(idx)
            values.append(float(r[idx + 3]))
    sparsed = Vectors.sparse(featuresLen, vectorIndexes, values)
    id = r[0]
    labelIndex = r[1]
    labelName = r[2]
    return (id, labelIndex, labelName, sparsed)
""")
        uid = generate_uuid()
        cmds.append('\n')
        #TROCAR STARTTIME POR ID
        cmds.append('df = {0}[{1}]\n'.format(self.get_input('dataframe'), '"' + '", "'.join(map(str.strip, ['StartTime'] + [self.get_input('label_col')] + self.get_input('features').split(','))) + '"'))
        #cmds.append('df.show(10)\n')
        if self.get_input('label_bot'):
            cmds.append("df = df.withColumn('{0}', regexp_replace('{0}', '[\w\W]*{1}[\w\W]*', 'bot'))\n".format(self.get_input('label_col'), self.get_input('label_bot')))
        if self.get_input('label_normal'):
            cmds.append("df = df.withColumn('{0}', regexp_replace('{0}', '[\w\W]*{1}[\w\W]*', 'normal'))\n".format(self.get_input('label_col'), self.get_input('label_normal')))
        if self.get_input('label_background'):
            cmds.append("df = df.withColumn('{0}', regexp_replace('{0}', '[\w\W]*{1}[\w\W]*', 'background'))\n".format(self.get_input('label_col'), self.get_input('label_background')))
        
        
        if not self.get_input('load_model'):
            pipelineStages = []
            cmds.append('feature_label = StringIndexer(inputCol="{0}", outputCol="labelIndex", handleInvalid="{1}").fit(df)\n'.format(self.get_input('label_col'), self.get_input('handle_invalid')))
            pipelineStages.append('feature_label')
            features = map(str.strip, self.get_input('features').split(','))
            discrete_features = map(str.strip, self.get_input('discrete_features').split(','))
            if discrete_features:
                for idx, feature in enumerate(discrete_features):
                    features = [f.replace(feature, feature+'Index') for f in features]
                    cmds.append('feature{0} = StringIndexer(inputCol="{1}", outputCol="{1}Index", handleInvalid="{2}").fit(df)\n'.format(idx, feature, self.get_input('handle_invalid')))
                    pipelineStages.append('feature{0}'.format(idx))
                
            cmds.append("model = Pipeline(stages=[{0}]).fit(df)\n".format(', '.join(pipelineStages)))
            cmds.append("df = model.transform(df)\n")
            if self.get_input('save_model'):
                #SALVAR LOCALMENTE!!!! NAS ARVORES TB
                # Como vou manter esse cara para todas as rodadas??Vou precisar adicionar o ID do app ou um UIID. Esse ID vai precisar aparecer la no resultado, para poder vincular aos modelos salvos...
                #Path hdfs://192.168.90.3:9000/user/tosta_hdfs/model_0 already exists. Please use write.overwrite().save(path) to overwrite it.
                #cmds.append("model.write.overwrite().save('{0}'.format(sc.applicationId))\n".format(self.get_input('save_model_dir') + 'feature_model_{0}'))
                cmds.append("model.write().overwrite().save('{0}')\n".format(self.get_input('save_model_dir')))
        else:
            features = map(str.strip, self.get_input('features').split(','))
            discrete_features = map(str.strip, self.get_input('discrete_features').split(','))
            if discrete_features:
                for idx, feature in enumerate(discrete_features):
                    features = [f.replace(feature, feature+'Index') for f in features]
            cmds.append("model = PipelineModel.read().load('{0}')\n".format(self.get_input('load_model_file')))  
            cmds.append("df = model.transform(df)\n")
            #cmds.append("df.show(20)\n")
            
        features_df = ['StartTime'] + ['labelIndex']+ ['{0}'.format(self.get_input('label_col'))] + features
        cmds.append('df = df[{0}]\n'.format('"' + '", "'.join(map(str.strip, features_df)) + '"'))
        cmds.append("df.show()\n")
        cmds.append('sparsed = df.rdd.map(lambda r: sparsefy(r, {0}))\n'.format(len(features)))
        cmds.append(uid + " = spark.createDataFrame(sparsed, ['id', 'labelIndex', 'labelName', 'features'])\n")
        #cmds.append(uid + ".show(30)\n")
            

        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('dataframe', uid)
        
class Referee(Module):
    _input_ports = [
        ('predictionCol', '(basic:String)', {'optional': False, 'defaults': "['prediction']"}),
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['labelIndex']"}),
        ('labelNameCol', '(basic:String)', {'optional': False, 'defaults': "['labelName']"}),
        ('predictions', '(basic:List)', {'optional': False})
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('predictions', '(basic:String)')
    ]

    def compute(self):
        cmds = []

        uid = generate_uuid()
        
        cmds.append("""
from collections import Counter

def mode(r):
    data = Counter([r[3],r[4],r[5]])
    a = 0
    return (data.most_common(1)[0][0] , r[0], r[1], r[2])
""")
        cmds.append('\n')
        #cmds.append('from pyspark.ml.feature import VectorAssembler\n')
        cmds.append('from pyspark.sql.functions import *\n')
        df1 = self.get_input('predictions')[0]
        df2 = self.get_input('predictions')[1]
        df3 = self.get_input('predictions')[2]
        cmds.append('{0}={0}["id", "prediction"]\n'.format(df2,))
        cmds.append('{0}={0}["id", "prediction"]\n'.format(df3,))
        #cmds.append('df_peso = {0}.withColumn("prediction", {0}.prediction + 0.1)\n'.format(df1,))
        cmds.append('joined = {0}.withColumnRenamed("prediction", "prediction1").join({1}.withColumnRenamed("prediction", "prediction2"), "id").join({2}.withColumnRenamed("prediction", "prediction3"), "id")\n'.format(df1,df2,df3))
        cmds.append('joined2 = joined["id", "labelIndex", "labelName", "prediction1", "prediction2", "prediction3"]\n')
        cmds.append('moded_df = joined2.rdd.map(lambda r: mode(r))\n')
        cmds.append(uid + '= spark.createDataFrame(moded_df, ["prediction", "id", "labelIndex", "labelName"])\n')
        #cmds.append(uid + '.show(20)\n')
        
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('predictions', uid)   
        self.set_output('output', self)


class StringIndexer(Module):
    _input_ports = [
        ('dataframe', '(basic:String)', {'optional': False}),
        ('inputCol', '(basic:String)', {'optional': False}),
        ('outputCol', '(basic:String)', {'optional': False}),
        ('handleInvalid', '(basic:String)', {'optional': True, 'defaults': "['skip']"})
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('transformer', '(basic:String)'),
        ('outputCol', '(basic:String)')
    ]

    def compute(self):
        cmds = []

        uid = generate_uuid()
        imports = 'from pyspark.ml.feature import StringIndexer\n'
        
        
        command = uid + ' = StringIndexer(inputCol="{0}", outputCol="{1}", handleInvalid ="{2}").fit({3})\n'.format(self.get_input('inputCol'), self.get_input('outputCol'), self.get_input('handleInvalid'), self.get_input('dataframe'))

        cmds.append(imports)
        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('transformer', uid)
        self.set_output('outputCol', self.get_input('outputCol'))


class ShowDataFrame(Module):
    _input_ports = [
        ('dataframe', '(basic:String)', {'optional': False}),
        ('num_lines', '(basic:Integer)', {'optional': False, 'defaults': "['10']"}),
    ]

    _output_ports = [
        ('output', '(basic:Module)')
    ]

    def compute(self):
        cmds = []
        
        command = "{0}.show({1})\n".format(self.get_input('dataframe'),self.get_input('num_lines'))

        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)

        
class IndexLabel(Module):
    _input_ports = [
        ('inputCol', '(basic:String)', {'optional': True, 'defaults': "['label']"}),
        ('outputCol', '(basic:String)', {'optional': True, 'defaults': "['indexedLabel']"}),
        ('data', '(basic:String)', {'optional': False}),
    ]

    _output_ports = [('output', '(basic:Module)'),
                     ('transformer', '(basic:String)'),
                     ('outputCol', '(basic:String)')
                     ]

    def compute(self):
        cmds = []

        uid = generate_uuid()
        imports = 'from pyspark.ml.feature import StringIndexer\n'
        command = uid + ' = StringIndexer(inputCol="{0}", outputCol="{1}").fit({2})\n'.format(self.get_input('inputCol'), self.get_input('outputCol'), self.get_input('data'))

        cmds.append(imports)
        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('transformer', uid)
        self.set_output('outputCol', self.get_input('outputCol'))


class IndexFeature(Module):
    _input_ports = [
        ('inputCol', '(basic:String)', {'optional': True, 'defaults': "['features']"}),
        ('outputCol', '(basic:String)', {'optional': True, 'defaults': "['indexedFeatures']"}),
        ('maxCategories', '(basic:Integer)', {'optional': False, 'defaults': "['10']"}),
        ('data', '(basic:String)', {'optional': False}),
    ]

    _output_ports = [('output', '(basic:Module)'),
                     ('transformer', '(basic:String)'),
                     ('outputCol', '(basic:String)')
                     ]

    def compute(self):
        cmds = []

        uid = generate_uuid()
        imports = 'from pyspark.ml.feature import VectorIndexer\n'
        command = uid + ' = VectorIndexer(inputCol="{0}", outputCol="{1}", maxCategories={2}).fit({3})\n'.format(self.get_input('inputCol'), self.get_input('outputCol'),
                                                                                                                 self.get_input('maxCategories'), self.get_input('data'))

        cmds.append(imports)
        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('transformer', uid)
        self.set_output('outputCol', self.get_input('outputCol'))


class DataSplit(Module):
    _input_ports = [
        ('train_perc', '(basic:Float)', {'optional': False, 'defaults': "['0.8']"}),
        ('data', '(basic:String)', {'optional': False}),
    ]

    _output_ports = [('output', '(basic:Module)'),
                     ('train', '(basic:String)'),
                     ('test', '(basic:String)'),
                     ]

    def compute(self):
        cmds = []

        uid = generate_uuid()
        uid1 = generate_uuid()
        uid2 = generate_uuid()

        command = '(' + uid1 + ',' + uid2 + ') = {0}.randomSplit([{1}, {2}])\n'.format(self.get_input('data'), self.get_input('train_perc'), 1 - self.get_input('train_perc'))

        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('train', uid1)
        self.set_output('test', uid2)


class Pipeline(Module):
    _input_ports = [
        ('mod1', '(basic:String)', {'optional': True}),
        ('mod2', '(basic:String)', {'optional': True}),
        ('mod3', '(basic:String)', {'optional': True}),
        # ('mod4', '(basic:String)', {'optional': False}),
        # ('mod5', '(basic:String)', {'optional': False})
    ]

    _output_ports = [('output', '(basic:Module)'),
                     ('pipeline', '(basic:String)')
                     ]

    def compute(self):
        cmds = []

        stages = []
        for input in self._input_ports:
            try:
                input = self.get_input(input[0])
                if input:
                    stages.append(input)
            except:
                pass
        stages_str = ', '.join(stages)

        uid = generate_uuid()
        imports = 'from pyspark.ml import Pipeline\n'
        command = uid + ' = Pipeline(stages=[{0}])\n'.format(stages_str, )

        cmds.append(imports)
        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('pipeline', uid)


class FitAndTransform(Module):
    _input_ports = [
        ('estimator', '(basic:String)', {'optional': False}),
        ('data', '(basic:String)', {'optional': False}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('dataframe', '(basic:String)'),
    ]

    def compute(self):
        cmds = []

        uid = generate_uuid()
        command = uid + ' = {0}.fit({1}).transform({1})\n'.format(self.get_input('estimator'), self.get_input('data'))

        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('dataframe', uid)


class Fit(Module):
    _input_ports = [
        ('estimator', '(basic:String)', {'optional': False}),
        ('train_data', '(basic:String)', {'optional': False}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('model', '(basic:String)'),
    ]

    def compute(self):
        global i
        cmds = []

        uid = generate_uuid()
        command = uid + ' = {0}.fit({1})\n'.format(self.get_input('estimator'), self.get_input('train_data'))
        cmds.append(command)
        #Path hdfs://192.168.90.3:9000/user/tosta_hdfs/model_0 already exists. Please use write.overwrite().save(path) to overwrite it.
        #command = uid + '.save("hdfs://192.168.90.3:9000/user/tosta_hdfs/model_{0}")\n'.format(i)
        cmds.append(command)
        
        i = i + 1
        
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('model', uid)


class Transform(Module):
    _input_ports = [
        ('model', '(basic:String)', {'optional': False}),
        ('test_data', '(basic:String)', {'optional': False}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('dataframe', '(basic:String)'),
    ]

    def compute(self):
        cmds = []

        uid = generate_uuid()
        command = uid + ' = {0}.transform({1})\n'.format(self.get_input('model'), self.get_input('test_data'))

        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('dataframe', uid)
        
class ParamGridBuilder(Module):
    # _input_ports = [
    #     ('pyspark_shell', '(basic:Module)', {'optional': False}),
    # ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('param_grid', '(basic:String)'),
    ]

    def compute(self):
        cmds = []

        imports = 'from pyspark.ml.tuning import ParamGridBuilder\n'

        uid = generate_uuid()
        command = uid + ' = ParamGridBuilder()\n'

        cmds.append(imports)
        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('param_grid', uid)


class addGrid(Module):
    _input_ports = [
        ('param_grid', '(basic:String)', {'optional': False}),
        ('target', '(basic:String)', {'optional': False}),
        ('param', '(basic:String)', {'optional': False}),
        ('param_values', '(basic:String)', {'optional': False}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('param_grid', '(basic:String)'),
    ]

    def compute(self):
        cmds = []

        param_grid = str(self.get_input('param_grid'))

        uid = generate_uuid()
        command = uid + ' = ' + param_grid + '.addGrid({0}.{1}, [{2}])\n'.format(str(self.get_input('target')), str(self.get_input('param')), str(self.get_input('param_values')))

        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('param_grid', uid)


class baseOn(Module):
    _input_ports = [
        ('param_grid', '(basic:String)', {'optional': False}),
        ('target', '(basic:String)', {'optional': False}),
        ('param', '(basic:String)', {'optional': False}),
        ('param_value', '(basic:String)', {'optional': False}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('param_grid', '(basic:String)'),
    ]

    def compute(self):
        cmds = []

        param_grid = str(self.get_input('param_grid'))

        uid = generate_uuid()
        command = uid + ' = ' + param_grid + '.baseOn([{0}.{1}, {2}])\n'.format(str(self.get_input('target')), str(self.get_input('param')), str(self.get_input('param_value')))

        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('param_grid', uid)


class MulticlassClassificationEvaluator(Module):
    _input_ports = [
        ('predictionCol', '(basic:String)', {'optional': False, 'defaults': "['prediction']"}),
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['labelIndex']"}),
        ('metricName', '(basic:String)', {'optional': False, 'defaults': "['accuracy']"})
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('evaluator', '(basic:String)'),
    ]

    def compute(self):
        cmds = []

        imports = 'from pyspark.ml.evaluation import MulticlassClassificationEvaluator\n'
        uid = generate_uuid()
        command = uid + ' = MulticlassClassificationEvaluator(predictionCol="{0}", labelCol="{1}", metricName="{2}")\n'.format(str(self.get_input('predictionCol')), str(self.get_input('labelCol')), str(self.get_input('metricName')))

        cmds.append(imports)
        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('evaluator', uid)

        
def find_between( s, first, last ):
    try:
        if s.find( first ) > -1:
            start = s.index( first ) + len( first )
            end = s.index( last, start )
            return s[start:end]
        return ""
    except ValueError:
        return ""
        
class MulticlassEvaluate(Module):
    _input_ports = [
        ('predictionCol', '(basic:String)', {'optional': False, 'defaults': "['prediction']"}),
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['labelIndex']"}),
        ('labelNameCol', '(basic:String)', {'optional': False, 'defaults': "['labelName']"}),
        ('predictions', '(basic:String)', {'optional': False})
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('evaluator', '(basic:String)'),
    ]

    def compute(self):
        cmds = []
        
        cmds.append('\n')
        cmds.append('from pyspark.mllib.evaluation import MulticlassMetrics\n')
        cmds.append('from pyspark.sql.functions import *\n')
        #cmds.append('{0}.sort(desc("probability")).show(20)\n'.format(self.get_input('predictions'),))
        #cmds.append('{0}.show(20)\n'.format(self.get_input('predictions'),))
        cmds.append('predictionAndLabels = {0}["{1}","{2}"].rdd\n'.format(self.get_input('predictions'), self.get_input('predictionCol'), self.get_input('labelCol')))
        #cmds.append('print predictionAndLabels.take(20)\n')
        cmds.append('metrics = MulticlassMetrics(predictionAndLabels)\n')
        cmds.append("labels = {0}.select('{1}', '{2}').distinct().collect()\n".format(self.get_input('predictions'), self.get_input('labelCol'), self.get_input('labelNameCol')))
        cmds.append("""
for label in labels:
    print "***###Precision--%s--%s+++" % (label[1], metrics.precision(label[0]))
    print "***###Recall--%s--%s+++" % (label[1], metrics.recall(label[0]))
    print "***###F1--%s--%s+++" % (label[1], metrics.fMeasure(label[0]))
    print "***###FalsePositive--%s--%s+++" % (label[1], metrics.falsePositiveRate(label[0]))
    print "***###TruePositive--%s--%s+++" % (label[1], metrics.truePositiveRate(label[0]))
""")
        cmds.append('\n')
        cmds.append('print "***###Accuracy--general--%s+++" % (metrics.accuracy,)\n')
        cmds.append('print "***###F1--general--%s+++" % (metrics.weightedFMeasure(),)\n')
        cmds.append('print "***###wFalsePositive--general--%s+++" % (metrics.weightedFalsePositiveRate,)\n')
        cmds.append('print "***###wTruePositive--general--%s+++" % (metrics.weightedTruePositiveRate,)\n')
        cmds.append('print "***###wPrecision--general--%s+++" % (metrics.weightedPrecision,)\n')
        cmds.append('print "***###wRecall--general--%s+++" % (metrics.weightedRecall,)\n')

        from sets import Set
        
        results = {}
        results['start'] = self.moduleInfo['job_monitor']._current_workflow.start
        
        metrics = []
        labels = Set([])
        for cmd in cmds:
            res = send_and_wait_cmd(cmd)
            for line in res.split('***'):
                if '+++' in line:
                    quase_res = line.strip().split('--')
                    if len(quase_res) == 3 and '%' not in line:
                        metric = find_between(line, '###', '+++').split('--')
                        metrics.append(metric)
                        labels.add(metric[1])
                        
        for label in labels:
            results[label] = {}
            for metric in metrics:
                if label == metric[1]:
                     results[label][metric[0]] = metric[2]
                     
        import pprint
        pprint.pprint(results)
        
        import json
        
        old_file = None
        try:
            old_file = open(self.moduleInfo['controller'].file_name + '.result.txt', 'r')
        except:
            pass

        if not old_file:
            last_res = []
        else:
            last_res = json.load(old_file)
            old_file.close()

        last_res.append(results)
        file = open(self.moduleInfo['controller'].file_name + '.result.txt', 'w')
        json.dump(last_res, file)
        file.close()

        self.set_output('output', self)
        self.set_output('evaluator', 'eval')

class Evaluate(Module):
    _input_ports = [
        ('evaluator', '(basic:String)', {'optional': False}),
        ('label', '(basic:String)', {'optional': False, 'defaults': "['bot']"}),
        ('transformed', '(basic:String)', {'optional': False})
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('result', '(basic:String)'),
    ]

    def compute(self):
        cmds = []

        uid = generate_uuid()
        command1 =  "preds = {0}.filter({0}['labelName']=='{1}')\n".format(self.get_input('transformed'), self.get_input('label'))
        command2 = uid + ' = {0}.evaluate(preds)\n'.format(self.get_input('evaluator'))
        cmds.append(command1)
        cmds.append(command2)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        print 'Evaluating - {0}... '.format(self.get_input('label'))
        buff = send_and_wait_cmd('print ' + uid + '\n')

        self.set_output('output', self)
        self.set_output('result', uid)


class CrossValidator(Module):
    _input_ports = [
        ('estimator', '(basic:String)', {'optional': False}),
        ('evaluator', '(basic:String)', {'optional': False}),
        ('params', '(basic:String)', {'optional': False}),
        ('num_folds', '(basic:String)', {'optional': False, 'defaults': "['3']"}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('estimator', '(basic:String)'),
    ]

    def compute(self):
        cmds = []

        imports = 'from pyspark.ml.tuning import CrossValidator\n'
        uid = generate_uuid()
        command = uid + ' = CrossValidator(estimator={0}, estimatorParamMaps={1}.build(), evaluator={2}, numFolds={3})\n'.format(str(self.get_input('estimator')), str(self.get_input('params')),
                                                                                                                                 str(self.get_input('evaluator')), str(self.get_input('num_folds')))

        cmds.append(imports)
        cmds.append(command)

        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('estimator', uid)


class RandomForestClassifier(Module):
    _input_ports = [
        ('dataframe', '(basic:String)', {'optional': False}),
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['labelIndex']"}),
        ('train_perc', '(basic:Float)', {'optional': False, 'defaults': "['0.8']"}),
        ('featuresCol', '(basic:String)', {'optional': False, 'defaults': "['features']"}),
        ('maxDepth', '(basic:Integer)', {'optional': True, 'defaults': "['5']"}),
        ('numTrees', '(basic:String)', {'optional': True, 'defaults': "['20']"}),
        ('maxBins', '(basic:Integer)', {'optional': True, 'defaults': "['32']"}),
        ('minInstancesPerNode', '(basic:Integer)', {'optional': True, 'defaults': "['1']"}),
        ('minInfoGain', '(basic:Float)', {'optional': True, 'defaults': "['0.0']"}),
        ('maxMemoryInMB', '(basic:Integer)', {'optional': True, 'defaults': "['256']"}),
        ('cacheNodeIds', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('checkpointInterval', '(basic:Integer)', {'optional': True, 'defaults': "['10']"}),
        ('impurity', '(basic:String)', {'optional': True, 'defaults': "['gini']"}),
        ('featureSubsetStrategy', '(basic:String)', {'optional': True, 'defaults': "['auto']"}),
        ('seed', '(basic:String)', {'optional': True, 'defaults': "['None']"}),
        ('save_model', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('save_model_dir', '(basic:String)', {'optional': True, 'defaults': "['hdfs://192.168.90.3:9000/user/tosta_hdfs/']"}),
        ('load_model', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('load_model_file', '(basic:String)', {'optional': True, 'defaults': "['hdfs://192.168.90.3:9000/user/tosta_hdfs/model_XPTO']"}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('predictions', '(basic:String)')
    ]

    def compute(self):
        uid = generate_uuid()
        cmds = []
        cmds.append('from pyspark.ml.classification import RandomForestClassifier\n')

        if not self.get_input('load_model'):
            params = []
            vars = ['maxDepth', 'maxBins', 'minInstancesPerNode', 'minInfoGain', 'maxMemoryInMB', 'cacheNodeIds', 'checkpointInterval', 'impurity', 'numTrees', 'featureSubsetStrategy', 'seed', 'labelCol', 'featuresCol']
            for var in vars:
                if (var in 'labelCol') or (var in 'featuresCol') or (var in 'impurity') or (var in 'featureSubsetStrategy'):
                    params.append('{0}="{1}"'.format(var, str(self.get_input(var))))
                else:
                    params.append('{0}={1}'.format(var, str(self.get_input(var))))

            cmds.append('rf = RandomForestClassifier( ' + ', '.join(params) + ')\n')
            cmds.append("df_ = {0}\n".format(self.get_input('dataframe')))
            cmds.append("labelCol_ = '{0}'\n".format(self.get_input('labelCol')))
            cmds.append("train_perc_ = {0}\n".format(self.get_input('train_perc')))
            cmds.append("labels = df_.select(labelCol_).distinct().collect()\n")
            cmds.append("""
train = None
test = None
for label in labels:
    if train == None:
        train, test = df_.filter(df_[labelCol_] == label['labelIndex']).randomSplit([train_perc_, 1 - train_perc_])
    else:
        temp_train, temp_tes = df_.filter(df_[labelCol_] == label['labelIndex']).randomSplit([train_perc_, 1 - train_perc_])
        train = train.union(temp_train)
        test = test.union(temp_tes)
""")
            cmds.append('\n')
            cmds.append('model = Pipeline(stages=[rf]).fit(train)\n')
            cmds.append(uid + ' = model.transform(test)\n')
            if self.get_input('save_model'):
                cmds.append("model.write().overwrite().save('{0}')\n".format(self.get_input('save_model_dir')))
        else:
            cmds.append("model = PipelineModel.read().load('{0}')\n".format(self.get_input('load_model_file')))  
            cmds.append(uid + ' = model.transform({0})\n'.format(self.get_input('dataframe')))
            #cmds.append(uid + '.show(20)\n')

        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('predictions', uid)


class DecisionTreeClassifier(Module):
    _input_ports = [
        ('dataframe', '(basic:String)', {'optional': False}),
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['labelIndex']"}),
        ('train_perc', '(basic:Float)', {'optional': False, 'defaults': "['0.8']"}),
        ('featuresCol', '(basic:String)', {'optional': False, 'defaults': "['features']"}),
        ('maxDepth', '(basic:Integer)', {'optional': True, 'defaults': "['5']"}),
        ('maxBins', '(basic:Integer)', {'optional': True, 'defaults': "['32']"}),
        ('minInstancesPerNode', '(basic:Integer)', {'optional': True, 'defaults': "['1']"}),
        ('minInfoGain', '(basic:Float)', {'optional': True, 'defaults': "['0.0']"}),
        ('maxMemoryInMB', '(basic:Integer)', {'optional': True, 'defaults': "['256']"}),
        ('cacheNodeIds', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('checkpointInterval', '(basic:Integer)', {'optional': True, 'defaults': "['10']"}),
        ('impurity', '(basic:String)', {'optional': True, 'defaults': "['gini']"}),
        ('save_model', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('save_model_dir', '(basic:String)', {'optional': True, 'defaults': "['hdfs://192.168.90.3:9000/user/tosta_hdfs/']"}),
        ('load_model', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('load_model_file', '(basic:String)', {'optional': True, 'defaults': "['hdfs://192.168.90.3:9000/user/tosta_hdfs/model_XPTO']"}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('predictions', '(basic:String)')
    ]

    def compute(self):
        uid = generate_uuid()
        cmds = []
        cmds.append('from pyspark.ml.classification import DecisionTreeClassifier\n')
        
        if not self.get_input('load_model'):
            params = []
            vars = ['maxDepth', 'maxBins', 'minInstancesPerNode', 'minInfoGain', 'maxMemoryInMB', 'cacheNodeIds', 'checkpointInterval', 'impurity', 'labelCol', 'featuresCol']
            for var in vars:
                if (var in 'labelCol') or (var in 'featuresCol') or (var in 'impurity'):
                    params.append('{0}="{1}"'.format(var, str(self.get_input(var))))
                else:
                    params.append('{0}={1}'.format(var, str(self.get_input(var))))

            cmds.append('dt = DecisionTreeClassifier( ' + ', '.join(params) + ')\n')
            cmds.append("df_ = {0}\n".format(self.get_input('dataframe')))
            cmds.append("labelCol_ = '{0}'\n".format(self.get_input('labelCol')))
            cmds.append("train_perc_ = {0}\n".format(self.get_input('train_perc')))
            cmds.append("labels = df_.select(labelCol_).distinct().collect()\n")
            cmds.append("""
train = None
test = None
for label in labels:
    if train == None:
        train, test = df_.filter(df_[labelCol_] == label['labelIndex']).randomSplit([train_perc_, 1 - train_perc_])
    else:
        temp_train, temp_tes = df_.filter(df_[labelCol_] == label['labelIndex']).randomSplit([train_perc_, 1 - train_perc_])
        train = train.union(temp_train)
        test = test.union(temp_tes)
""")
            cmds.append('\n')
            cmds.append('model = Pipeline(stages=[dt]).fit(train)\n')
            cmds.append(uid + ' = model.transform(test)\n')
            cmds.append(uid + '.cache()\n')
            if self.get_input('save_model'):
                cmds.append("model.write().overwrite().save('{0}')\n".format(self.get_input('save_model_dir')))
        else:
            cmds.append("model = PipelineModel.read().load('{0}')\n".format(self.get_input('load_model_file')))  
            cmds.append(uid + ' = model.transform({0})\n'.format(self.get_input('dataframe')))
            cmds.append(uid + '.cache()\n')
            cmds.append(uid + '.count()\n')

        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('predictions', uid)

        
class DecisionTree(Module):
    _input_ports = [
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['label']"}),
        ('featuresCol', '(basic:String)', {'optional': False, 'defaults': "['features']"}),
        ('maxDepth', '(basic:Integer)', {'optional': True, 'defaults': "['5']"}),
        ('maxBins', '(basic:Integer)', {'optional': True, 'defaults': "['32']"}),
        ('minInstancesPerNode', '(basic:Integer)', {'optional': True, 'defaults': "['1']"}),
        ('minInfoGain', '(basic:Float)', {'optional': True, 'defaults': "['0.0']"}),
        ('maxMemoryInMB', '(basic:Integer)', {'optional': True, 'defaults': "['256']"}),
        ('cacheNodeIds', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('checkpointInterval', '(basic:Integer)', {'optional': True, 'defaults': "['10']"}),
        ('impurity', '(basic:String)', {'optional': True, 'defaults': "['gini']"})
    ]

    _output_ports = [
        ('output', '(basic:String)'),
        ('classifier', '(basic:String)'),
        ('params', '(basic:String)')
    ]

    def compute(self):
        cmds = []

        vars = ['maxDepth', 'maxBins', 'minInstancesPerNode', 'minInfoGain', 'maxMemoryInMB', 'cacheNodeIds', 'checkpointInterval', 'impurity', 'labelCol', 'featuresCol']

        imports = 'from pyspark.ml.classification import DecisionTreeClassifier\n'
        uid = generate_uuid()

        params = []

        for var in vars:
            if (var in 'labelCol') or (var in 'featuresCol') or (var in 'impurity'):
                params.append('{0}="{1}"'.format(var, str(self.get_input(var))))
            else:
                params.append('{0}={1}'.format(var, str(self.get_input(var))))

        command = uid + ' = DecisionTreeClassifier( ' + ', '.join(params) + ')\n'

        cmds.append(imports)
        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('classifier', uid)
        
class MultilayerPerceptronClassifier(Module):
    _input_ports = [
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['label']"}),
        ('featuresCol', '(basic:String)', {'optional': False, 'defaults': "['features']"}),
        ('maxIter', '(basic:Integer)', {'optional': True, 'defaults': "['5']"}),
        ('tol', '(basic:Float)', {'optional': True, 'defaults': "['1e-4']"}),
        ('seed', '(basic:Float)', {'optional': True, 'defaults': "['1']"}),
        ('layers', '(basic:List)', {'optional': True, 'defaults': "['[9, 5, 5, 3]']"}),
        ('blockSize', '(basic:Integer)', {'optional': True, 'defaults': "['128']"}),
    ]

    _output_ports = [
        ('output', '(basic:String)'),
        ('classifier', '(basic:String)'),
        ('params', '(basic:String)')
    ]

    def compute(self):
        cmds = []

        vars = ['maxIter', 'tol', 'layers', 'blockSize', 'seed', 'labelCol', 'featuresCol']

        imports = 'from pyspark.ml.classification import MultilayerPerceptronClassifier\n'
        uid = generate_uuid()

        params = []

        for var in vars:
            if (var in 'labelCol') or (var in 'featuresCol') or (var in 'solver'):
                params.append('{0}="{1}"'.format(var, str(self.get_input(var))))
            else:
                params.append('{0}={1}'.format(var, str(self.get_input(var))))

        command = uid + ' = MultilayerPerceptronClassifier( ' + ', '.join(params) + ')\n'

        cmds.append(imports)
        cmds.append(command)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('classifier', uid)
             
class LogisticRegressionClassifier(Module):
    _input_ports = [
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['label']"}),
        ('featuresCol', '(basic:String)', {'optional': False, 'defaults': "['features']"}),
        ('maxIter', '(basic:Integer)', {'optional': True, 'defaults': "['100']"}),
        ('regParam', '(basic:Float)', {'optional': True, 'defaults': "['0.0']"}),
        ('elasticNetParam', '(basic:Float)', {'optional': True, 'defaults': "['0.0']"}),
        ('tol', '(basic:Float)', {'optional': True, 'defaults': "['1e-6']"}),
        ('fitIntercept', '(basic:Boolean)', {'optional': True, 'defaults': "['True']"}),
        ('threshold', '(basic:Float)', {'optional': True, 'defaults': "['0.5']"}),
        ('standardization', '(basic:Boolean)', {'optional': True, 'defaults': "['True']"}),
    ]

    _output_ports = [
        ('output', '(basic:String)'),
        ('classifier', '(basic:String)'),
        ('params', '(basic:String)')
    ]

    def compute(self):
        cmds = []

        vars = ['maxIter', 'regParam', 'elasticNetParam', 'tol', 'fitIntercept', 'threshold', 'standardization', 'labelCol', 'featuresCol']

        imports1 = 'from pyspark.ml.classification import LogisticRegression\n'
        imports2 = 'from pyspark.ml.classification import OneVsRest\n'
        uid = generate_uuid()

        params = []

        for var in vars:
            if (var in 'labelCol') or (var in 'featuresCol') or (var in 'weightCol'):
                params.append('{0}="{1}"'.format(var, str(self.get_input(var))))
            else:
                params.append('{0}={1}'.format(var, str(self.get_input(var))))

        command1 = 'lr = LogisticRegression( ' + ', '.join(params) + ')\n'
        command2 = uid + ' = OneVsRest(featuresCol="{0}", labelCol="{1}", classifier=lr)\n'.format(self.get_input('featuresCol'), self.get_input('labelCol'))

        cmds.append(imports1)
        cmds.append(imports2)
        cmds.append(command1)
        cmds.append(command2)
        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('classifier', uid)

        
class NaiveBayesClassifier(Module):
    _input_ports = [
        ('dataframe', '(basic:String)', {'optional': False}),
        ('labelCol', '(basic:String)', {'optional': False, 'defaults': "['labelIndex']"}),
        ('train_perc', '(basic:Float)', {'optional': False, 'defaults': "['0.8']"}),
        ('featuresCol', '(basic:String)', {'optional': False, 'defaults': "['features']"}),
        ('smoothing', '(basic:Float)', {'optional': True, 'defaults': "['1.0']"}),
        ('modelType', '(basic:String)', {'optional': True, 'defaults': "['multinomial']"}),
        #('thresholds', '(basic:List)', {'optional': True, 'defaults': "[['None']"})
        ('save_model', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('save_model_dir', '(basic:String)', {'optional': True, 'defaults': "['hdfs://192.168.90.3:9000/user/tosta_hdfs/']"}),
        ('load_model', '(basic:Boolean)', {'optional': True, 'defaults': "['False']"}),
        ('load_model_file', '(basic:String)', {'optional': True, 'defaults': "['hdfs://192.168.90.3:9000/user/tosta_hdfs/model_XPTO']"}),
    ]

    _output_ports = [
        ('output', '(basic:Module)'),
        ('predictions', '(basic:String)')
    ]

    def compute(self):
        cmds = []
        cmds.append('from pyspark.ml.classification import NaiveBayes\n')
        cmds.append('from pyspark.ml.classification import OneVsRest\n')
        uid = generate_uuid()
        if not self.get_input('load_model'):
            params = []
            vars = ['smoothing', 'modelType', 'labelCol', 'featuresCol']
            for var in vars:
                if (var in 'labelCol') or (var in 'featuresCol')or (var in 'modelType'):
                    params.append('{0}="{1}"'.format(var, str(self.get_input(var))))
                else:
                    params.append('{0}={1}'.format(var, str(self.get_input(var))))

            cmds.append('nb = NaiveBayes( ' + ', '.join(params) + ')\n')
            cmds.append('ovr = OneVsRest(featuresCol="{0}", labelCol="{1}", classifier=nb)\n'.format(self.get_input('featuresCol'), self.get_input('labelCol')))
            cmds.append("df_ = {0}\n".format(self.get_input('dataframe')))
            cmds.append("labelCol_ = '{0}'\n".format(self.get_input('labelCol')))
            cmds.append("train_perc_ = {0}\n".format(self.get_input('train_perc')))
            cmds.append("labels = df_.select(labelCol_).distinct().collect()\n")
            cmds.append("""
train = None
test = None
for label in labels:
    if train == None:
        train, test = df_.filter(df_[labelCol_] == label['labelIndex']).randomSplit([train_perc_, 1 - train_perc_])
    else:
        temp_train, temp_tes = df_.filter(df_[labelCol_] == label['labelIndex']).randomSplit([train_perc_, 1 - train_perc_])
        train = train.union(temp_train)
        test = test.union(temp_tes)
""")
            cmds.append('\n')
            cmds.append('model = Pipeline(stages=[ovr]).fit(train)\n')
            cmds.append(uid + ' = model.transform(test)\n')
            if self.get_input('save_model'):
                cmds.append("model.write().overwrite().save('{0}')\n".format(self.get_input('save_model_dir')))
        else:
            cmds.append("model = PipelineModel.read().load('{0}')\n".format(self.get_input('load_model_file')))  
            cmds.append(uid + ' = model.transform({0})\n'.format(self.get_input('dataframe')))

        for cmd in cmds:
            send_and_wait_cmd(cmd)

        self.set_output('output', self)
        self.set_output('predictions', uid)

_modules = [MultilayerPerceptronClassifier, NaiveBayesClassifier, LogisticRegressionClassifier, LoadNetworkFlows, ShowDataFrame, StringIndexer, SelectFeaturesAndLabel, PySparkShell, ParamGridBuilder, addGrid, baseOn, CrossValidator, Evaluate, MulticlassClassificationEvaluator, MulticlassEvaluate, IndexLabel, IndexFeature, DataSplit,
            Pipeline, Fit, Transform, RandomForestClassifier, DecisionTreeClassifier, Referee]
