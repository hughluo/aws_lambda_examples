import json
import csv
import boto3
import urllib.request


def lambda_handler(event, context):
    bucket_name = "wexort-output"
    key = "res1"
    API_prefix = "https://postman-echo.com/get?"
    tmp_path = "/tmp/" + key
    fieldnames = ["website", "visits"]
    
    urls = parse_trigger(API_prefix)
    response_dict_list = bulk_call_api_to_response_dict_list(urls)
    write_dict_list_as_cvs(response_dict_list, fieldnames, tmp_path)
    upload_csv_to_s3(bucket_name, key, tmp_path)

def parse_trigger(API_prefix):
    
    rows = ["www.google.com", "www.shit.com", "www.aha.com"] * 30
    res = map(lambda url : API_prefix + "website=" + url + "&visits=" + "123", rows)
    return res
    
def bulk_call_api_to_response_dict_list(urls):
    res = []
    for url in urls:
        response = urllib.request.urlopen(url)

        if response.getcode() == 200:
            response_dict = response_content_to_dict(response.read())
            res.append(response_dict)
    
    return res


def response_content_to_dict(response_content):
    
    json_data = json.loads(response_content)
        
    response_dict = {"website": json_data["args"]["website"], "visits": json_data["args"]["visits"]}
        
    return response_dict

def write_dict_list_as_cvs(dict_list, fieldnames, tmp_path):
    #Only then you can write the data into the '/tmp' folder.
    with open(tmp_path, 'w', newline='') as f:
        
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        for d in dict_list:
            writer.writerow(d)
            

def upload_csv_to_s3(bucket_name, key, tmp_path):
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(tmp_path, bucket_name, key)
