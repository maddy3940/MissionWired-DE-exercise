# MissionWired-DE-exercise

This repository contains my solution to the challenge posed by MissionWired. In this exercise, a dataset simulating CRM data is available in some public AWS S3 files - 
- [Constituent Information](https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv)
- [Constituent Email Addresses](https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv)
- [Constituent Subscription Status](https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv)


## Pipeline Architecture

![image](https://user-images.githubusercontent.com/44323045/220688039-9e6c7e4c-fae6-4aa2-ba08-43ad22e935f1.png)

### ETL steps
In this architecture following steps are carried out-

- On running the scripts.py file, data is fetched from the S3 buckets.
- Primary Email ids are filtered out from cons_email data frame
- Then two data frames are created from cons_subscription_status
    - Email Ids who are subscribed to chapter 1 
        - Includes email ids from cons_subscription_status who have subscribed to chapter 1 (where isunsub = 0)
        - Includes email ids from cons_information that are not present in cons_subscription_status
    - Email Ids who are not subscribed to chapter 1
        - Includes email ids from cons_subscription_status who are not subscribed to chapter 1 (where isunsub = 1)
        - Includes email ids from cons_subscription_status who are subscribed/unsubscribed to other chapter except chapter 1
 - Combine the above two dataframe and filter rows where email id is primary
 - Join cons_information dataframe to get other output columns
 - Save people.csv in current working directory
 - Use people.csv file, extract date from created_dt
 - Group by date and aggregate counts of email ids to get counts of people acquired on that day
 - Save acquisition_facts.csv to current working directory

- Detailed and technical comments about the logic along with an explanation has been added in this [Jupyter Notebook](https://github.com/maddy3940/MissionWired-DE-exercise/blob/main/DE_exercise.ipynb)

### Automation steps

-  In this solution I have considered scenario where the CRM data will be updated on a weekly basis
-  CI/CD pipeline is set up on this Github repo using Git Actions which will trigger execution of scripts.py on every new push to the repo and every week at Sunday midnight UTC time
-  Once CI/CD pipeline is executed, updated file will be replaced in the git repo as well as on [Github pages](https://maddy3940.github.io/MissionWired-DE-exercise/data)


## Output files

-  Business users can quickly download the weekly updated output files [here](https://maddy3940.github.io/MissionWired-DE-exercise/data)

## How to reproduce the results

### Steps to run Scripts locally

- Required system dependencies:

    - Python 3.7+ installation or IDE
    - Jupyter to run notebooks

- Open up terminal after Python installation and run the command -
```python
pip install -r requirements.txt
```
- Once installed you can open a jupyter notebook and run cells from this [Jupyter Notebook](https://github.com/maddy3940/MissionWired-DE-exercise/blob/main/DE_exercise.ipynb) or you can run the command to run the script.py file and save the results in your current working directory
```python
python script.py
```
### Steps to run Script on Github Codespace
- This repo is configured to run on Github Codespace. Anyone will be able to run the script on Github codespace once I have added them as collaborators to this repo
- Click on Code button on this repo and hit codespaces as shown below
![image](https://user-images.githubusercontent.com/44323045/220700911-a19f2a76-21c1-4580-97f3-e9d2242a87e3.png)
- Codespace will spin up a VM with all the dependencies required to run the script.py file in this repo
- Once the VS code window opens up select the script.py file and hit run. You should be able to play around with the code using browser itself
![image](https://user-images.githubusercontent.com/44323045/220702255-17c02b84-704c-4b68-829d-777079794a27.png)



## Output File Data Dictionary
- People.csv
  ![image](https://user-images.githubusercontent.com/44323045/220690367-5e030e74-fec4-4755-b3a6-eaa002545521.png)

- acquisition_facts.csv

  ![image](https://user-images.githubusercontent.com/44323045/220690467-4c228d51-da69-42d3-a98b-6590dfd4b182.png)


## Assumptions Made
- In the file cons_email_chapter_subscription.csv, I have assumed that when isunsub is 1 it means the email id is not subscribed to chapter and if 0 it means email id is subscribed
- Since the output data dictionary for people.csv that is given says that the column email is the primary email address, I have filtered out only primary email addresses from Constituent Email Addresses where is_primary =1
- Code column in the people.csv table is same as source column in Constituent Information and similarly created_dt and updated_dt are fields extracted from Constituent Information where the columns are named - create_dt and modified_dt respectively
- While creating the acquisition_facts.csv I simply aggregated the counts of email ids subscribed/ unsubscribed on that particular date. My intuition said to only consider people who have subscribed on that date (i.e isunsub =0) but since it was not clearly stated anywhere, I simply aggregated the counts of email ids that were subscribed/unsubcribed on a particular date 
