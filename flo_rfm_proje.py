import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x : '%3f' % x)
pd.set_option('display.width', 500)
df_ = pd.read_csv('datasets/flo_data_20k.csv')
df = df_.copy()
df.head()

'''                             master_id order_channel last_order_channel first_order_date last_order_date last_order_date_online last_order_date_offline  order_num_total_ever_online  order_num_total_ever_offline  customer_value_total_ever_offline  customer_value_total_ever_online       interested_in_categories_12  total_order  total_order_value
0  cc294636-19f0-11eb-8d74-000d3a38a36f   Android App            Offline       2020-10-30      2021-02-26             2021-02-21              2021-02-26                     4.000000                      1.000000                         139.990000                        799.380000                           [KADIN]     5.000000         939.370000
1  f431bd5a-ab7b-11e9-a2fc-000d3a38a36f   Android App             Mobile       2017-02-08      2021-02-16             2021-02-16              2020-01-10                    19.000000                      2.000000                         159.970000                       1853.580000  [ERKEK, COCUK, KADIN, AKTIFSPOR]    21.000000        2013.550000
2  69b69676-1a40-11ea-941b-000d3a38a36f   Android App        Android App       2019-11-27      2020-11-27             2020-11-27              2019-12-01                     3.000000                      2.000000                         189.970000                        395.350000                    [ERKEK, KADIN]     5.000000         585.320000
3  1854e56c-491f-11eb-806e-000d3a38a36f   Android App        Android App       2021-01-06      2021-01-17             2021-01-17              2021-01-06                     1.000000                      1.000000                          39.990000                         81.980000               [AKTIFCOCUK, COCUK]     2.000000         121.970000
4  d6ea1074-f1f5-11e9-9346-000d3a38a36f       Desktop            Desktop       2019-08-03      2021-03-07             2021-03-07              2019-08-03                     1.000000                      1.000000                          49.990000                        159.990000                       [AKTIFSPOR]     2.000000         209.980000
'''

df.columns

df.describe().T

df.isnull().sum()

df.info()

df['total_order'] = df['order_num_total_ever_online'] + df['order_num_total_ever_offline']
df['total_order_value'] = df['customer_value_total_ever_offline'] + df['customer_value_total_ever_online']

a = df.loc[:, df.columns.str.contains('date')].columns

for i in a:
    df[i] = pd.to_datetime(df[i])

df.describe().T

df.groupby('master_id').agg({'total_order_value': 'sum'}).sort_values('total_order_value', ascending=False).head(10)
df.groupby('master_id').agg({'total_order': 'sum'}).sort_values('total_order', ascending=False).head(10)

today_date = dt.datetime(2021, 6, 2)

rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                   'total_order': lambda total_order: total_order.sum(),
                                   'total_order_value': lambda total_order_value: total_order_value.sum()})

rfm.columns = ['recency', 'frequency', 'monetary']

rfm["recency_score"] = pd.qcut(rfm['recency'], 5 , labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) +
                    rfm['frequency_score'].astype(str))

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}
rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

rfm.head()

'''                                      recency  frequency    monetary recency_score frequency_score monetary_score RFM_SCORE      segment
master_id                                                                                                                               
00016786-2f5a-11ea-bb80-000d3a38a36f       11   5.000000  776.070000             5               4              4        54    champions
00034aaa-a838-11e9-a2fc-000d3a38a36f      299   3.000000  269.470000             1               2              1        12  hibernating
000be838-85df-11ea-a90b-000d3a38a36f      214   4.000000  722.690000             2               3              4        23      at_Risk
000c1fe2-a8b7-11ea-8479-000d3a38a36f       28   7.000000  874.160000             5               4              4        54    champions
000f5e3e-9dde-11ea-80cd-000d3a38a36f       21   7.000000 1620.330000             5               4              5        54    champions'''


'''Case_1
 
FLO includes a new women's shoe brand. The product prices of the brand it includes are above the general customer preferences. 
For this reason, it is desired to contact the customers in the profile that will be interested in the promotion of the brand and product sales. 
Those who shop from their loyal customers (champions, loyal_customers) and women category are the customers to be contacted specifically. 
Save the id numbers of these customers to the csv file.'''

SEGMENT_A = rfm[(rfm["segment"] == "champions") | (rfm["segment"] == "loyal_customers")]
SEGMENT_A.shape[0]

SEGMENT_B = df[(df["interested_in_categories_12"]).str.contains("KADIN")]
SEGMENT_B.shape[0]

one_case = pd.merge(SEGMENT_A, SEGMENT_B[["interested_in_categories_12", "master_id"]], on=["master_id"])

one_case.columns

one_case = one_case.drop(one_case.loc[:, 'recency':'interested_in_categories_12'].columns, axis=1)


one_case.to_csv("case_2_customers.csv")


''' Case_2
Nearly 40% discount is planned for Men's and Children's products. It is aimed to specifically target customers who are good customers in the past, 
but who have not shopped for a long time, who are interested in the categories related to this discount, who should not be lost, those who are asleep and new customers. 
Save the ids of the customers in the appropriate profile to the csv file.'''

SEGMENT_C = rfm[
    (rfm["segment"] == "cant_loose") | (rfm["segment"] == "about_to_sleep") | (rfm["segment"] == "new_customers")]

SEGMENT_D = df[(df["interested_in_categories_12"]).str.contains("ERKEK|COCUK")]

second_case = pd.merge(SEGMENT_C, SEGMENT_D[["interested_in_categories_12", "master_id"]], on=["master_id"])

second_case = second_case.drop(second_case.loc[:, 'recency':'interested_in_categories_12'].columns, axis=1)

second_case.to_csv("case_2_customers.csv", index=False)