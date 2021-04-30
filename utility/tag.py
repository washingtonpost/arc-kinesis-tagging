import requests
import os
from dotenv import load_dotenv
load_dotenv()


def setup_requests():
    headers = {'Authorization': 'Bearer ' + os.environ.get('arc_dev_token')}
    url = 'https://api.{0}.arcpublishing.com/draft/v1'.format(os.environ.get('arc_org_name'))
    return headers, url

def get_document(_id):
    headers, url = setup_requests()
    response = requests.get(url+'/story/{0}/revision/draft'.format(_id), headers=headers)
    document = response.json()
    ans = document.get('ans')
    return ans

def update_document(_id, ans):
    headers, url = setup_requests()
    req_body = {
        "document_id": _id,
        "ans": ans,
        "type": "DRAFT"
    }

    response = requests.put(url+'/story/{0}/revision/draft'.format(_id), json=req_body, headers=headers)
    put_response = response
    return 

def modify_ans(ans, tag): 
    # once your tag is decided, this method will go add it correctly do the ANS. 
    if not ans.get('taxonomy'):
        document_tags = [tag]
        ans['taxonomy']['tags'] = document_tags
    else: 
        document_tags = ans.get('taxonomy').get('tags')
        if document_tags:
            if not tag in document_tags:
                document_tags.append(tag)
        else:
            document_tags = [tag]
        
        ans['taxonomy']['tags'] = document_tags
    
    return ans


def autotag(record):
    _id = record.get('_id')
    ans = get_document(_id)
    
    # in this example, we are adding the same tag to all published stories. 
    # in your consumer, you will want to modify this section to perform whatever
    # business logic you need to perform before deciding how to tag this story. 
    # this could be calling out to a third party service, etc. 
    # if this method is going to take a long time, you may want to consider making
    # this function asynchronous. https://realpython.com/python-async-features/ 
    tag_to_add = {
            'slug': "kinesis-autotag",
            'text': 'kinesis autotag'
        }

    ans = modify_ans(ans, tag_to_add)
    update_document(_id, ans)
    return 