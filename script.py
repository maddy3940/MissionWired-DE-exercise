import warnings
warnings.filterwarnings("ignore")
import os
import pandas as pd
pd.set_option('display.max_columns', None)

def subtract_df(df1, df2, on):
    """
    Subtracts the rows of df2 from df1 based on a common column.

    Parameters:
    df1 (pandas.DataFrame): The DataFrame to subtract from.
    df2 (pandas.DataFrame): The DataFrame to subtract.
    on (str or list of str, optional): The column(s) to use as the join key(s). If not specified, the function will use all common columns.

    Returns:
    pandas.DataFrame: The resulting DataFrame after subtracting df2 from df1.
    """
    # merge the DataFrames
    result = df1.merge(df2, on=on, how='left',suffixes=('','_y'), indicator=True)

    # select only the rows in df1
    result = result[result['_merge'] == 'left_only']

    # drop the indicator column
    result = result.drop(['_merge'] + [col for col in result.columns if '_y' in col], axis=1)

    return result

def main():
    cons_information = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv')
    cons_email = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv')
    cons_subscription = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv')

    print("Read all the files from S3")

    # Taking only subset of the file that will be required
    cons_email_subset = cons_email[['cons_email_id','cons_id','is_primary','email']]
    cons_subscription_subset = cons_subscription[['cons_email_id','chapter_id','isunsub']]
    cons_information_subset = cons_information[['cons_id','source','create_dt','modified_dt']]

    # Filtering only primary email ids
    cons_email_primary = cons_email_subset.loc[cons_email_subset['is_primary']==1].reset_index().drop(['index'],axis=1)

    # Filtering out email ids who have subscribed to chapter 1
    cons_subscription_chap1_sub = cons_subscription_subset.loc[(cons_subscription_subset['chapter_id']==1) & (cons_subscription_subset['isunsub']==0)].reset_index()
    # Filtering out email ids who have unsubscribed to chapter 1
    cons_subscription_chap1_notsub = cons_subscription_subset.loc[(cons_subscription_subset['chapter_id']==1) & (cons_subscription_subset['isunsub']==1)].reset_index()
    # Filtering out email ids where chapter is not 1
    cons_subscription_notchap1 = cons_subscription_subset.loc[cons_subscription_subset['chapter_id']!=1].reset_index()

    # Filtering out email ids that are subscribed to chapter 1 and other chapters
    cons_subscription_commonchap = cons_subscription_notchap1.merge(cons_subscription_chap1_sub, on='cons_email_id',suffixes=('','_y'))
    cons_subscription_commonchap = cons_subscription_commonchap.drop([col for col in cons_subscription_commonchap.columns if '_y' in col], axis=1)

    cons_subscription_notsubchap1 = subtract_df(cons_subscription_notchap1,cons_subscription_commonchap,on='cons_email_id').reset_index().drop(['level_0'],axis=1)

    emails_not_sub_chap1_final = pd.concat([cons_subscription_notsubchap1,cons_subscription_chap1_notsub],axis=0)
    emails_not_sub_chap1_final = emails_not_sub_chap1_final.drop_duplicates(subset=['cons_email_id'])

    # Removing email ids that are present in constitunet_subscription_status
    email_in_primary_sub_chap1 = subtract_df(cons_email_primary,cons_subscription_subset,on='cons_email_id').reset_index().drop(['is_primary','email','cons_id'], axis=1)
    email_in_primary_sub_chap1['chapter_id']=1
    email_in_primary_sub_chap1['isunsub']=0

    # Combining people who have subscribed to chapter 1
    emails_sub_chap1_final = pd.concat([email_in_primary_sub_chap1,cons_subscription_chap1_sub],axis=0).reset_index().drop(['level_0'], axis=1)

    peoples_final = pd.concat([emails_sub_chap1_final,emails_not_sub_chap1_final],axis=0).reset_index().drop(['level_0'], axis=1)

    # Joining the primary constitunet_email dataframe on cons_email_id 
    people_final_primary_email = cons_email_primary.merge(peoples_final,on='cons_email_id').drop(['index','is_primary','chapter_id'],axis=1)

    # Joining with constituent information dataframe to get the other columns
    final_df =  people_final_primary_email.merge(cons_information_subset,on='cons_id').drop(['cons_email_id','cons_id'],axis=1)

    final_df = final_df.rename(columns={'create_dt':'created_dt','modified_dt':'updated_dt','source':'code','isunsub':'is_unsub'})[['email','code','is_unsub','created_dt','updated_dt']]

    final_df.to_csv('people.csv',header=True,index=False)

    print("Saved people.csv file")


    final_df['acquisition_date'] = pd.to_datetime(final_df['created_dt']).dt.date

    acquisition_facts = final_df.groupby(final_df['acquisition_date']).agg({'acquisition_date':'count'}).rename(columns={'acquisition_date': 'count'}).reset_index()

    acquisition_facts.to_csv('acquisition_facts.csv',header=True,index=False)

    print("Saved acquisition_facts.csv file")


if __name__ == '__main__':
    main()