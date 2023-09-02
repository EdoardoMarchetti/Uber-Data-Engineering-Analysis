import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        df: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    #Remove duplicates 
    df = df.drop_duplicates().reset_index(drop=True)

    #Create rip id
    df['trip_id'] = df.index
    
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    #Datatime_dimension
    print('Step 1 - Datetime dimension')
    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday


    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index

    datetime_dim = datetime_dim[['datetime_id']+list(datetime_dim.columns)[:-1]]

    #Passenger_count_dimension
    print('Step 2 - passenger_count_dim')
    passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

    #Trip_dimension
    print('Step 3 - trip_distance_dim')
    trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

    #Ratecode_dimension
    print('Step 4 - ratecode_dim')
    ratecode_names_list = ['Standard rate', 'JFK', 'Newark', 'Nassua or Westchester', 'Negotiated fare', 'Group ride'] 
    ratecode_names_dic = {i+1:name for i,name in enumerate(ratecode_names_list)}

    ratecode_dim = df[['RatecodeID']].reset_index(drop=True)
    ratecode_dim['rate_code_id'] = ratecode_dim.index
    ratecode_dim['ratecode_name'] = ratecode_dim['RatecodeID'].map(ratecode_names_dic)
    ratecode_dim = ratecode_dim[['rate_code_id', 'RatecodeID', 'ratecode_name']]

    #Location dimensions
    print('Step 5 - pickup_location_dim')
    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_longitude', 'pickup_latitude']]
    print('Step 6 - pickup_location_dim')
    drop_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    drop_location_dim['dropoff_location_id'] = drop_location_dim.index
    drop_location_dim = drop_location_dim[['dropoff_location_id', 'dropoff_longitude', 'dropoff_latitude']]
    
    #Payment dimension
    print('Step 7 - payment_type_names_list')
    payment_type_names_list = ['Credit card', 'Cash','No charge', 'Dispute', 'Unkown', 'Voided trip']
    payment_type_names_dict = {i:name for i, name in enumerate(payment_type_names_list)}

    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_names_dict)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim = payment_type_dim[['payment_type_id']+list(payment_type_dim.columns)[:-1]]
    
    #Fact table dimension
    print('Step 8 - fact_table')
    fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
             .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
             .merge(ratecode_dim, left_on='trip_id', right_on='rate_code_id') \
             .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
             .merge(drop_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
             .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
             .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]
    

    return {
        "datetime_dim":datetime_dim.to_dict(orient='dict'),
        "passenger_count_dim":passenger_count_dim.to_dict(orient='dict'),
        "trip_distance_dim":trip_distance_dim.to_dict(orient='dict'),
        "ratecode_dim":ratecode_dim.to_dict(orient='dict'),
        "pickup_location_dim":pickup_location_dim.to_dict(orient='dict'),
        "drop_location_dim":drop_location_dim.to_dict(orient='dict'),
        "payment_type_dim":payment_type_dim.to_dict(orient='dict'),
        "fact_table":fact_table.to_dict(orient='dict'),
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
