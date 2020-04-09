import pandas as pd
import json

def get_maximum_values(event, context):
    data = pd.read_csv('s3://therealgraphs.com/representative.csv')
    maximum_encoded_day = data['EncodedDay'].max()
    maximum_active_cases = data['Active'].max()
    return {
        "statusCode": 200,
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true',
        },
        'body': json.dumps({
                                "maximum_encoded_day": str(maximum_encoded_day),
                                "maximum_active_cases": str(maximum_active_cases)
                           })
    }

if __name__ == "__main__":
    get_maximum_values('', '')
