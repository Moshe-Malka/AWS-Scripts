import logging
import boto3
import re
import pandas as pd
import io

class S3Operations:
    def __init__(self, **kwargs):
        self.session = boto3.Session(**kwargs, region_name='us-east-1')
        self._s3_resource = self.session.resource('s3')
        self._s3_client = self.session.client('s3')
        self.region_name = kwargs.get('region_name', 'us-east-1')

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('allcloud-s3-package')

        self.allowd_signs = ['>', '<']
    
    def _isnotebook(self):
        try:
            get_ipython()
            return True
        except Exception:
            return False

    def _humanbytes(self, _bytes):
        '''Return the given bytes as a human friendly KB, MB, GB, or TB string'''
        _bytes = float(_bytes)
        kb = float(1024)
        mb = float(kb ** 2) # 1,048,576
        gb = float(kb ** 3) # 1,073,741,824
        tb = float(kb ** 4) # 1,099,511,627,776

        if _bytes < kb:
            return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
        elif kb <= _bytes < mb:
            return f'{(_bytes/kb):.2f} KB'
        elif mb <= _bytes < gb:
            return f'{(_bytes/mb):.2f} MB'
        elif gb <= _bytes < tb:
            return f'{(_bytes/gb):.2f} GB'
        elif tb <= _bytes:
            return f'{(_bytes/tb):.2f} TB'
        else: return '<-Error->'

    def _filter_by_pattern(self, _list, pat, key=None):
        try:
            pat = pat.replace('*', '.*')
            reg = re.compile(pat)
            if key:
                return list(filter(lambda x: bool(re.search(reg, x[key])), _list))
            else:
                return list(filter(lambda x: bool(re.search(reg, x)), _list))
        except Exception as e:
            self.logger.error(f'Error trying to filter list by pattren : {pat}.\n{e}')

    def _filter_by_size(self, _list, size_str):
        try:
            size = size_str[1:]
            if size_str[0] == '>':
                return list(filter(lambda x: x['Size'] > size, _list))
            elif size_str[0] == '<':
                return list(filter(lambda x: x['Size'] > size, _list))
            else:
                self.logger.error(f'Error trying to filter by size - invalid sign : {size_str}.')
        except Exception as e:
            self.logger.error(f'Error trying to filter by size.\n{e}')

    def create_bucket(self, bkt_name):
        try:
            response = self._s3_client.create_bucket( Bucket=bkt_name )
            self.logger.info(response)
        except Exception as e:
            print (e)
            self.logger.error(f'Error trying to create Bucket with name {bkt_name} - {e}')

    def list_buckets(self, pat=None):
        try:
            response = self._s3_client.list_buckets()['Buckets']
            self.logger.info(response)
            buckets = [x['Name'] for x in response]
            if pat:
                return self._filter_by_pattern(buckets, pat)
            else:
                return buckets
        except Exception as e:
            self.logger.error(f'Error trying to list Buckets with pattren {pat} - {e}')

    def list_bucket_content(self, bkt_name, fname_pat=None, size_str=None):
        try:
            response = self._s3_client.list_objects_v2( Bucket=bkt_name )['Contents']
            if len(response) == 0: return []
            files = [ { 'Key' : x['Key'], 'Size' : x['Size']} for x in response]
            if fname_pat:
                files = self._filter_by_pattern(files, fname_pat, 'Key')
            if size_str:
                if size_str[0] not in self.allowd_signs: raise Exception('Error - Unknow/no sign in size argument.')
                files = self._filter_by_size(filterd, size_str)
            formmated = [ { 'Key' : x['Key'], 'Size' : self._humanbytes(x['Size'])} for x in files ]
            return formmated
        except Exception as e:
            self.logger.error(f'Error trying to list Bucket content with filters: filename : {fname_pat}, size : {size_str}.\n{e}')
        
    def preview_file(self, bkt_name, fname, row_limit=None):
        try:
            obj = self._s3_client.get_object(Bucket=bkt_name, Key=fname)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            if row_limit:
                df = df.iloc[:row_limit, :]
            with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                if self._isnotebook(): display(df)
                else: print(df)
        except Exception as e:
            self.logger.error(f'Error - failed to preview file : {fname} - {e}')

    def copy(self, from_bkt, from_key, to_bkt, to_key):
        try:
            source = { 'Bucket': from_bkt , 'Key': from_key }
            target = self._s3_resource.Bucket(to_bkt).Object(to_key)
            target.copy(source)
            self.logger.info('Succesfully copied file.')
        except Exception as e:
            self.logger.error(f'Error - failed to copy file/s.\n{e}')

    def query_file(self, bkt_name, fname, exp):
        try:
            response = self._s3_client.select_object_content(
                Bucket=bkt_name,
                Key=fname,
                ExpressionType='SQL',
                Expression=exp,
                InputSerialization={
                    'CSV': { 'FileHeaderInfo': 'USE', 'RecordDelimiter': '\n', 'FieldDelimiter': ',' }
                },
                OutputSerialization= {
                    'CSV': {'RecordDelimiter': '\n', 'FieldDelimiter': ',' }
                }
            )
            self.logger.info(response)
            table = []
            for event in response['Payload']:
                if 'Records' in event:
                    row = event['Records']['Payload'].decode('utf-8')
                    table.append(row.split(','))
            return table
        except Exception as e:
            self.logger.error(f'Error - failed to query file : {fname} - {e}')
            return []

# Tests
# if __name__ == '__main__':    
#     s3_actions = S3Operations()
    # print(s3_actions.list_buckets())
    # s3_actions.create_bucket('testing-moshe-malka')
    # print(s3_actions.list_buckets())
    # print(s3_actions.list_buckets('allcloud*'))
    # print(s3_actions.query_file( 'query-results-trip-data-allcloud', 'Unsaved/2020/03/23/33608481-d20b-408a-af8a-65c7ff9bcd0a.csv', 'SELECT * FROM s3object'))
    # s3_actions.preview_file('query-results-trip-data-allcloud', 'Unsaved/2020/03/23/33608481-d20b-408a-af8a-65c7ff9bcd0a.csv')
    # s3_actions.preview_file('query-results-trip-data-allcloud', 'Unsaved/2020/03/23/33608481-d20b-408a-af8a-65c7ff9bcd0a.csv', 10)
    # s3_actions.copy('query-results-trip-data-allcloud', 'Unsaved/2020/03/23/33608481-d20b-408a-af8a-65c7ff9bcd0a.csv', 'lambda-layeres', 'testing-folder/ourfile.csv')


