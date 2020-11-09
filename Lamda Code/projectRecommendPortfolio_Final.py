### Required Libraries ###
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import boto3
import json



# Global Variables

etf_count = 5
etf_list  = "SPY, AGG, DIA, XLY, GLD, SLV, EFA, VWO, USO, QQQ"
fi_models = "ARIMA and LSTM"

### Functionality Helper Functions ###
def parse_int(n):
    """
    Securely converts a non-integer value to integer.
    """
    try:
        return int(n)
    except ValueError:
        return float("nan")


def build_validation_result(is_valid, violated_slot, message_content):
    """
    Define a result message structured as Lex response.
    """
    if message_content is None:
        return {"isValid": is_valid, "violatedSlot": violated_slot}

    return {
        "isValid": is_valid,
        "violatedSlot": violated_slot,
        "message": {"contentType": "PlainText", "content": message_content},
    }



def validate_data(age, investment_amount, intent_request, etfs, investment_period):
    """
    Validates the data provided by the user.
    """
    # Validate the retirement age based on the user's current age.
    # Age is capped at 75 years for portfolio recommendations.
    if age is not None:
        age = parse_int(
            age
        )  # Since parameters are strings it's important to cast values
        if age <= 20:
            return build_validation_result(
                False,
                "age",
                "Your age is invalid, can you provide an age greater than 20?",
            )
        elif age > 75:
            return build_validation_result(
                False,
                "age",
                "The maximum age to contract this service is 75, "
                "can you provide an age between 21 and 75 please?",
            )
    # Validate the investment amount, it should be >= 5000
    if investment_amount is not None:
        investment_amount = parse_int(investment_amount)
        if investment_amount < 5000:
            return build_validation_result(
                False,
                "investmentAmount",
                "The minimum investment amount is $5,000 USD, "
                "could you please provide a greater amount?",
            )
        
 
    # Validate the number of ETFs, it should be 1 to 10
    if etfs is not None:
        etfs = parse_int(etfs)
        if etfs <= 0:
            return build_validation_result(
                False,
                "etfs",
                "Your entry for etfs is invalid, can you provide a number greater than 0?",
            )
        elif etfs > 10:
            return build_validation_result(
                False,
                "etfs",
                "The maximum ETFs to select is 10, "
                "can you provide a number between 1 and 10 please?",
            )
 
    # Validate the investment period, it should be 1 to 365

    if investment_period is not None:
        investment_period = parse_int(
            investment_period
        )  # Since parameters are strings it's important to cast values
        if investment_period <= 0:
            return build_validation_result(
                False,
                "investmentPeriod",
                "Your Investment Period is invalid, can you provide a value greater than 0?",
            )
        elif investment_period > 365:
            return build_validation_result(
                False,
                "investmentPeriod",
                "Please choose from 1 to 365 as Investment Period",
            )

    return build_validation_result(True, None, None)


### Dialog Actions Helper Functions ###
def get_slots(intent_request):
    """
    Fetch all the slots and their values from the current intent.
    """
    return intent_request["currentIntent"]["slots"]


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    """
    Defines an elicit slot type response.
    """

    return {
        "sessionAttributes": session_attributes,
        "dialogAction": {
            "type": "ElicitSlot",
            "intentName": intent_name,
            "slots": slots,
            "slotToElicit": slot_to_elicit,
            "message": message,
        },
    }


def delegate(session_attributes, slots):
    """
    Defines a delegate slot type response.
    """

    return {
        "sessionAttributes": session_attributes,
        "dialogAction": {"type": "Delegate", "slots": slots},
    }


def close(intent_request, session_attributes, fulfillment_state, message):
    """
    Defines a close slot type response.
    """
    print('yaya')
    
    intent_name=intent_request['currentIntent']['name']
    model_type=intent_request["currentIntent"]["slots"]["projectModelType"]
    number=parse_int(intent_request["currentIntent"]["slots"]["etfs"])
    num_days=parse_int(intent_request["currentIntent"]["slots"]["investmentPeriod"])
    style=intent_request["currentIntent"]["slots"]["projectSentiment"]
    
    if intent_name=="ProjectRecommendPortfolio" and style=='Stocks' and model_type=='ARIMA':
        print("Inside")
        df=pd.read_csv('https://arimacsv1.s3-us-west-2.amazonaws.com/arima.csv')
        each_return=df.iloc[0:number, 1]-1
        each_ticker=df.iloc[0:number, 0]
        print(each_ticker.tolist())
        multiplier=1/number
        final_return=sum(multiplier*each_return)*100
        message['content']=f'Your suggested ETFs are {each_ticker.tolist()}. Assuming equal weighting, your return over the next {number} days will be {final_return}'
    elif intent_name=="ProjectRecommendPortfolio" and style=='News':
        ENDPOINT_NAME = 'sigmodata-sentiment-6-264f9eeab0e7b478e-2020-08-15-12-33-38-083'
        runtime= boto3.client('runtime.sagemaker')
        s3_client = boto3.client('s3')
    
        request = s3_client.get_object(Bucket = 'jason-test-2.0-sagemaker', Key = 'pol_article_text.csv')
        request_bytes_articles_pol=request['Body'].read().decode('utf-8')
        response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                       ContentType='text/csv',
                                       Body=request_bytes_articles_pol)
                                       
        x=response['Body'].read()
   
        x=x.decode()
    
        y=x.split("\n")
        y=[[g] for g in y]
    
        negs=0
        pos=0
        totalP=0
        totalN=0
        neg_val=[]
        pos_val=[]
        for elem in y:
       
            elemList=elem[0].split(",")
       
            if len(elemList)>1:
                if "positive" in elemList[-2]:
                    jj=elemList[-1]
                    jj=jj.strip().strip('"')
                    pos+=1
                    totalP+=float(jj)
                    #pos_val.append(elemList[-1])
                elif "negative" in elemList[-2]:
                    jk=elemList[-1]
                    jk=jk.strip().strip('"')
                 
                    totalN+=float(jk)
                    negs+=1
                    #neg_val.append(elemList[-1])
    
    
        quo=float(pos)/float(negs)
        certaintyP=float(totalP/pos)
        certaintyN=float(totalN/negs)
        message['content']=f'There are {quo} times as many positive stories as negative stories. The average certainty for positive stories was {certaintyP} vs {certaintyN} for negative stories'
        
        
        
    #print(message)
    elif intent_name=="ProjectRecommendPortfolio" and style=='Stocks' and model_type=='LSTM':
        ENDPOINT_NAME = 'DEMO-deepar-2020-08-18-03-14-52-883'
        runtime= boto3.client('runtime.sagemaker')
        df=pd.read_csv("https://s3-us-west-2.amazonaws.com/jason-test-2.0-sagemaker/stocks.csv")
    
        df.index=df['Date']
        df.drop(columns='Date', inplace=True)
        df.index=pd.to_datetime(df.index)
        
        df.dropna(inplace=True)
        finalList={}
        for x,y in df.iteritems():
        
            spyList=y.tolist()
            
            payload={"start": str(df.index[0]), "target": spyList}
   
    
    
    
            req={
            'instances': [payload],
                'configuration': {"num_samples": 1, "output_types": ["quantiles"], "quantiles": ["0.5"]}
                }
            
            req=json.dumps(req).encode('utf-8')
            resultList=1
            
            for z in range(num_days):
            
                response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                       ContentType='application/json',
                                       Body=req)
                                       
   
                result = json.loads(response['Body'].read().decode())
       
                my_prediction=result['predictions'][0]['quantiles']['0.5'][0]
                if x=='VWO':
                    print('INSIDE')
                    print(my_prediction)
                
                resultList*=float(1+my_prediction)
                if x=='VWO':
                    print(resultList)
            totalReturn=(resultList-1)
            finalList[x]=[totalReturn]
        print("HEYEYEYOOOOO")
        print(finalList)
       
        df_final=pd.DataFrame(finalList)
        df_final=df_final.T
        print("HEYEYEYORRRRRRRRRRR")
       
        print(df_final)
        df_final=df_final.sort_values(by=0, ascending=False)
        
        each_return=df_final.iloc[0:number, 0]
        each_ticker=df_final.index[0:number]
        print(each_ticker.tolist())
        print(each_return)
        multiplier=1/number
        final_return=sum(multiplier*each_return)*100
        print(final_return)
        message['content']=f'Your suggested ETFs are {each_ticker.tolist()}. Assuming equal weighting, your return over the next {num_days} days will be {final_return}'
                #message['content']=f"My prediction for your cumulative return over the next {num_days} days is {totalReturn}%"
        
        
    response = {
        "sessionAttributes": session_attributes,
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": fulfillment_state,
            "message": message,
        },
    }

    return response


### Intents Handlers ###
def recommend_portfolio(intent_request):
    """
    Performs dialog management and fulfillment for recommending a portfolio.
    """

    first_name = get_slots(intent_request)["firstName"]
    age = get_slots(intent_request)["age"]
    investment_amount = get_slots(intent_request)["investmentAmount"]
    risk_level = get_slots(intent_request)["projectRiskLevel"]
    etfs = get_slots(intent_request)["etfs"]
    investment_period = get_slots(intent_request)["investmentPeriod"]
    model_type = get_slots(intent_request)["projectModelType"]
    sentiment = get_slots(intent_request)["projectSentiment"]
    source = intent_request["invocationSource"]
    

    if source == "DialogCodeHook":
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt
        # for the first violation detected.

        ### YOUR DATA VALIDATION CODE STARTS HERE ###
               
        slots = get_slots(intent_request)
        validation_result = validate_data(age, investment_amount, intent_request, etfs, investment_period)
        if not validation_result["isValid"]:
            slots[validation_result["violatedSlot"]] = None  # Cleans invalid slot
            # Returns an elicitSlot dialog to request new data for the invalid slot
            return elicit_slot(
                intent_request["sessionAttributes"],
                intent_request["currentIntent"]["name"],
                slots,
                validation_result["violatedSlot"],
                validation_result["message"],
            )
        
        ### YOUR DATA VALIDATION CODE ENDS HERE ###

        # Fetch current session attibutes
        output_session_attributes = intent_request["sessionAttributes"]

        return delegate(output_session_attributes, get_slots(intent_request))

    # Get the initial investment recommendation

    ### YOUR FINAL INVESTMENT RECOMMENDATION CODE STARTS HERE ###
    initial_recommendation = get_investment_recommendation(risk_level)

    ### YOUR FINAL INVESTMENT RECOMMENDATION CODE ENDS HERE ###

    # Return a message with the initial recommendation based on the risk level.
    return close(
        intent_request,
        intent_request["sessionAttributes"],
        "Fulfilled",
        {
            "contentType": "PlainText",
            "content": """{} thank you for your information;
            based on the risk level you defined, my recommendation is to choose an investment portfolio with {}
            """.format(
                first_name, initial_recommendation
            ),
        },
    )

def get_investment_recommendation(risk_level):
    """
    Returns an initial investment recommendation based on the risk profile.
    """
    risk_levels = {
        "none": "100% bonds (AGG), 0% equities (SPY)",
        "very low": "80% bonds (AGG), 20% equities (SPY)",
        "low": "60% bonds (AGG), 40% equities (SPY)",
        "medium": "40% bonds (AGG), 60% equities (SPY)",
        "high": "20% bonds (AGG), 80% equities (SPY)",
        "very high": "0% bonds (AGG), 100% equities (SPY)",
    }
    return risk_levels[risk_level.lower()]

def finance_info(intent_request):
    """
    Performs fulfillment for listing the ETFs
    """
    
    """
    print("From Function finance_info:")
    print(" INTENT REQUEST IS")
    print(intent_request)
    print("Invocation Source is: ")    
    print(intent_request['invocationSource'])
    

    print("Our ETF List consists of these 10 entries")
    print(etf_list)
    """
    
    
    if intent_request['invocationSource']=='DialogCodeHook':
        return delegate(intent_request['sessionAttributes'], get_slots(intent_request))
    

    # Return a message with the list of ETFs
    return close(
        intent_request,
        intent_request["sessionAttributes"],
        "Fulfilled",
        {
            "contentType": "PlainText",
            "content": f"Our list has these  10 ETFs:  {etf_list}"
            
        }
    )


def how_many_etfs(intent_request):
    """
    Performs fulfillment for Number of ETFs
    """
    
    """
    print("From Function how_many_etfs:")
    print(" INTENT REQUEST IS")
    print(intent_request)
    print("Invocation Source is: ")    
    print(intent_request['invocationSource'])
    """


    if intent_request['invocationSource']=='DialogCodeHook':
        
        return delegate(intent_request['sessionAttributes'], get_slots(intent_request))
    

      # Return a message with the number of ETFs
    return close(
        intent_request,
        intent_request["sessionAttributes"],
        "Fulfilled",
        {
            "contentType": "PlainText",
            "content": f"Our list contains Top performing {etf_count} out of select list of these 10 ETFs: {etf_list}"
            
        }
    )

def project_models(intent_request):
    """
    Performs fulfillment for Models
    """
    print("From Function project_models:")
    print(" INTENT REQUEST IS")
    print(intent_request)
    print("Invocation Source is: ")    
    print(intent_request['invocationSource'])
    


    if intent_request['invocationSource']=='DialogCodeHook':
        
        return delegate(intent_request['sessionAttributes'], get_slots(intent_request))
    
    
    
       # Return a message with the Finance Models
    return close(
        intent_request,
        intent_request["sessionAttributes"],
        "Fulfilled",
        {
            "contentType": "PlainText",
            "content": f"We use {fi_models} for our recommendations"
            
        }
    )


### Intents Dispatcher ###
def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """
    print("KS, Current Intent Request: ", intent_request)
    intent_name = intent_request["currentIntent"]["name"]
    print("KS, Current Intent: ",  intent_name)

    # Dispatch to bot's intent handlers
    if intent_name == "ProjectRecommendPortfolio":
        return recommend_portfolio(intent_request)
        
    elif intent_name == "ProjectFinanceInfo":
        return finance_info(intent_request)    
        
    elif intent_name == "ProjectHowManyETFs":
        return how_many_etfs(intent_request) 
        
    elif intent_name == "ProjectModels":
        return project_models(intent_request)        

    raise Exception("Intent with name " + intent_name + " not supported")


### Main Handler ###
def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """

    return dispatch(event)
