
query="""
SELECT  ds
            FROM    onecomp_risk.ads_fin_rsk_fe_ent_rel_data_version
            WHERE   ds = MAX_PT("onecomp_risk.ads_fin_rsk_fe_ent_rel_data_version");
"""
import os
from odps import ODPS
from odps.accounts import StsAccount
# Make sure environment variable ALIBABA_CLOUD_ACCESS_KEY_ID already set to acquired Access Key ID,
# environment variable ALIBABA_CLOUD_ACCESS_KEY_SECRET set to acquired Access Key Secret
# while environment variable ALIBABA_CLOUD_STS_TOKEN set to acquired STS token.
# Not recommended to hardcode Access Key ID or Access Key Secret in your code.
o = ODPS(
    os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID'),
    os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET'),
    project='onecomp',
    endpoint='http://service-corp.odps.aliyun-inc.com/api',
)

with o.execute_sql(query).open_reader() as reader:
    pd_df = reader.to_pandas()
    print(pd_df['ds'][0])
