# %%
import pandas as pd

# %%
""" Get stuff out of Netfile v2 API
"""
from pprint import PrettyPrinter
from pathlib import Path
import os
import requests

BASE_URL = 'https://netfile.com/api/campaign'
CONTRIBUTION_FORM = 'F460A'
EXPENDITURE_FORM = 'F460E'

PARAMS = { 'aid': 'COAK' }

def get_auth_from_env_file(filename: str='.env'):
    """ Split .env file on newline and look for API_KEY and API_SECRET
        Return their values as a tuple
    """
    env_file=Path(filename)
    auth_keys = [ 'API_KEY', 'API_SECRET' ]
    if env_file.exists():
        auth = tuple( v for _, v in sorted([
            ln.split('=') for ln in
            env_file.read_text(encoding='utf8').strip().split('\n')
            if ln.startswith(auth_keys[0]) or ln.startswith(auth_keys[1])
        ], key=lambda ln: auth_keys.index(ln[0])))
    else:
        auth=tuple(os.environ[key] for key in auth_keys)
            
    return auth

AUTH=get_auth_from_env_file()

pp = PrettyPrinter()

def get_filing(offset=0):
    """ Get a filing
    """
    url = f'{BASE_URL}/filing/v101/filings'

    params = { **PARAMS }
    if offset > 0:
        params['offset'] = offset

    res = requests.get(url, params=params, auth=AUTH)
    if res.status_code == 500:
        print('ping')
        return get_filing(offset=0)
    else:
        print(res)
        body = res.json()
        results = body.pop('results')
        return results, body
def get_form(form,offset=0):
    """ Get a filing
    """
    url = f'{BASE_URL}/filing/v101/filings?Limit=100000&SpecificationForm={form}'

    params = { **PARAMS }
    if offset > 0:
        params['offset'] = offset

    res = requests.get(url, params=params, auth=AUTH)
    if res.status_code == 500:
        return get_form(form,offset=0)
    else:
        body = res.json()
        results = body.pop('results')

        return results, body
def get_filer(filer_nid):
    """ Get one filer
    """
    url = f'{BASE_URL}/filer/v101/filers?'

    res = requests.get(url, params={ **PARAMS, 'filerNid': filer_nid }, auth=AUTH)
    if res.status_code == 500:
        return get_filer(filer_nid)
    else:
        body = res.json()

        return body['results']
def list_filers():
    """ Get all the elections
    """
    url = f'{BASE_URL}/filer/v101/filers?Limit=100000'

    res = requests.get(url, params=PARAMS, auth=AUTH)
    if res.status_code == 500:
        print('ping')
        return list_filers()
    else:
        body = res.json()
        return body['results']
def list_elections_influences(id):
    """ Get all the elections
    """
    url = f'{BASE_URL}/election/v101/election-influences?Limit=100000&ElectionNid={id}'

    res = requests.get(url, params=PARAMS, auth=AUTH)
    if res.status_code == 500:
        return list_elections_influences(id)
    else:
        body = res.json()
        return body['results']

def list_elections():
    """ Get all the elections
    """
    url = f'{BASE_URL}/election/v101/elections?Limit=100000'

    res = requests.get(url, params=PARAMS, auth=AUTH)
    if res.status_code == 500:
        return list_elections()
    else:
        body = res.json()

        return body['results']
def export_transactions(id,offset=0):
    """ Get a filing
    """
    url = f'{BASE_URL}/filing/v101/filings/{id}'

    params = { **PARAMS }
    if offset > 0:
        params['offset'] = offset

    res = requests.get(url, params=params, auth=AUTH)
    if res.status_code == 500:
        return export_transactions(id,offset=0)
    else:
        body = res.json()

        return body

# %%
filers_response=list_filers()
filers_response

# %%
status = [
    {
        'isTerminated': item.get('isTerminated', {}),
        # Get the latest status and conditons for epmty lists to avoid indexError
        'status': item.get('statusItemList', None)[-1]['status'] if item.get('statusItemList', None) else None,
        'filerNid': item['filerNid'],
        'Filer Name':item['filerName'],
        'Filer Type': item['committeeTypes']
    } 
    for item in filers_response
]
status_df = pd.DataFrame(status)
# status_df

# %%
# get city, state, and zip from Disclosure addresses
addresses=[{'addressList':item.get('addressList',{}),'filerNid':item['filerNid']} for item in filers_response]
address_dic = {'city':[],'state':[],'zip':[],'filerNid':[]}
for item in addresses:
    for address in item['addressList']:
        if 'Disclosure' in address['addressTypes']:
            address_dic['city'].append(address['city'])
            address_dic['state'].append(address['state'])
            address_dic['zip'].append(address['zip'])
            address_dic['filerNid'].append(item['filerNid'])
address_df=pd.DataFrame(address_dic)
# address_df

# %%
# merge on filer id
status_address_df = status_df.merge(address_df,how='left', on='filerNid')

# %%
# associate a filer id to a fppc id
regs=[{'fppc_id':item.get('registrations',{}).get('CA SOS',None),'filerNid':item['filerNid']} for item in filers_response]
# get all filers with officers
officers=[[item['officers'], item['filerNid']] for item in filers_response if item['officers']]
# set up dictionary
treasurer_dic={}
# loop through filers with officers and add offcier names if officer position is treasurer the key will be the filler id
for officer in officers:
    if officer[0][0]['position']=='Treasurer':
        treasurer_dic[officer[1]]=officer[0][0]['officerName']
# match the filer id key in treasurer dic with the filer ids associated with an fppc id
for key, value in treasurer_dic.items():
    for item in regs:
        if key==item['filerNid']:
            # if a key matches a filer id then add treasure name to the dictionaries
            item['Treasurer']=value
# get only the cases with a treasurer
fppc_with_treasurer=[reg for reg in regs if reg.get('Treasurer', None)]
treasurer_df=pd.DataFrame(fppc_with_treasurer)
# treasurer_df

# %%
# I want to associate fppc ids with filer nids, I look through 410s, 501s, and filers_response.
# get ids from 410s
form410s=get_form('FPPC410')
form410s=form410s[0]
form410={'filerNid':[],'fppc_id':[]}
for form in form410s:
    form410['filerNid'].append(form['filerMeta']['filerId'])
    form410['fppc_id'].append(form.get('filerMeta',{}).get('strings',{}).get('Registration_CA SOS',None))

# %%
# get ids from 501s
form501s=get_form('FPPC501')
form501s=form501s[0]
for form in form501s:
    form410['filerNid'].append(form['filerMeta']['filerId'])
    form410['fppc_id'].append(form.get('filerMeta',{}).get('strings',{}).get('Registration_CA SOS',None))

# %%
# get ids from filer_response
for item in filers_response:
    form410['filerNid'].append(item.get('filerNid',{}))
    form410['fppc_id'].append(item.get('registrations',{}).get('CA SOS',None))
df_410=pd.DataFrame(form410)
df_410.drop_duplicates(inplace=True)
# In this data filer id can be associated with an ffpc id, a null value, or 'pending', I filter it for the best results then concatenate the best results last
# then drop duplicates keeping last since I put the prefferable rows last
# drop null values
t2=df_410.dropna()
# drop pending, this now only has preferable rows
t1=t2[t2['fppc_id'] != 'Pending']
duped=pd.concat([df_410, t2, t1], ignore_index=True)
best=duped.drop_duplicates(subset=['filerNid'],keep='last',inplace=False)
# best

# %%
# list all elections
elections=list_elections()
# elections

# %%
# from the list_elections response we ... 
election_list=[]
previous_df=pd.DataFrame()
for election in elections:
    # collect 
    candidates=election['candidates']
    seats=election['seats']
    election_name=election['electionCaption']
    electionNid=election['electionNid']
    # get the year from the four first character, the format is yyyy-mm-dd
    election_year=election['electionDate'][:4]
    election_key={'election_name':election_name, 'electionNid':electionNid, 'election year':election_year}
    election_list.append(election_key)
    if candidates and seats:
        seat_df=pd.DataFrame(seats)
        candidate_df=pd.DataFrame(candidates)
        merge_df=candidate_df.merge(seat_df, on='seatNid')
        current_df=merge_df[['candidateNid','candidateName','seatNid','officeName','electionNid','isIncumbent','isWinner']]
        previous_df=pd.concat([previous_df,current_df],ignore_index=True)
election_df=pd.DataFrame(election_list)
final_df=previous_df.merge(election_df, on='electionNid')
final_df.tail()

# %%
final_df

# %%
election_ids=list(set(final_df['electionNid'].to_list()))
previous_df=pd.DataFrame()
for id in election_ids:
    influences=list_elections_influences(id)
    influences_dic={'filerNid': [],'electionNid': [],'seatNid': [],'candidateNid': [],'committeeName':[],'election_name': []}
    for candidate in influences:   
        influences_dic['filerNid'].append(candidate.get('filerNid', 'None'))
        influences_dic['election_name'].append(candidate.get('electionCaption', 'None'))
        influences_dic['committeeName'].append(candidate.get('committeeName', 'None'))
        influences_dic['electionNid'].append(candidate.get('electionNid', 'None'))
        influences_dic['seatNid'].append(candidate.get('seatNid', 'None'))
        influences_dic['candidateNid'].append(candidate.get('candidateNid', 'None'))
        current_df=pd.DataFrame(influences_dic)
        current_df=current_df
        previous_df=pd.concat([previous_df,current_df],ignore_index=True)

# %%
df3=previous_df
# df3

# %%
dfNew = final_df.merge(df3,how='left', on=['candidateNid','election_name','electionNid','seatNid'])

# %%
core_df=dfNew[['candidateName','officeName','committeeName','election_name','filerNid','election year']] 
df4=core_df.merge(best, how='left',on=['filerNid']).drop_duplicates(ignore_index=True)

# %%
# merge treasurer with fppc id and filer id as keys
df5 = df4.merge(treasurer_df, how='left',on=['fppc_id','filerNid'])
# merge status and location with filer id as key
df6 = df5.merge(status_address_df, how='left',on=['filerNid'])
# rename columns for consistent style
df6.columns = ['candidate_name','office_name', 'committee_name', 'election_name', 'filler_nid', 'election_year', 'fppc_id', 'treasurer', 'is_terminated', 'status', 'filer_name', 'filer_type', 'city', 'state', 'zip']
df6

# %%
# output table as csv
df6.to_csv('output/output.csv')


