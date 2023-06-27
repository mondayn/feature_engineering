from datetime import timedelta

from feast import (
    Entity,
    FeatureStore,
    FeatureView,
    Field,
    FileSource,
)

from feast.types import (
    Float64, 
    Int64
)

from pathlib import Path


print('running ',__file__)

# load store
feature_store_yaml_location = Path(__file__).parent #os.path.dirname(__file__)
store = FeatureStore(feature_store_yaml_location)

# create entity
passenger = Entity(name="passenger", join_keys=["PassengerId"])
store.apply(passenger)

# define source
passenger_data_source = FileSource(
    name="passenger_data",
    path=str(Path(__file__).parent / 'data/titanic_features.parquet'),
    timestamp_field="event_timestamp"
)
store.apply(passenger_data_source)

# define feature view
passengers_fv = FeatureView(
    name="passenger_stats",
    entities=[passenger],
    ttl=timedelta(days=1),
    # 'PassengerId', 'Fare', 'Pclass', 'event_timestamp', 'female', 'male'
    schema=[
        Field(name="Fare", dtype=Float64),
        Field(name="Pclass", dtype=Int64),
        Field(name="encoded_title", dtype=Int64),
        Field(name="female", dtype=Int64),
        Field(name="male", dtype=Int64),
    ],
    online=True,
    source=passenger_data_source,
    tags={"team": "passenger_features"},
)
store.apply(passengers_fv)




####################### old code to create titanic_features.parquet
def is_str_in_list(s,lst):
    ''' returns empty list if str is not in lst
        str_in_list('this',['this','that'])    
    '''
    return list(filter(lambda x: x.lower() in s.lower(),lst))

def parse_and_encode_title(s):
    if is_str_in_list(s,['Mrs','Miss','Countes','Mlle','Mme','Lady']):
        return 1
    if is_str_in_list(s,['Sir','Col','Major','Dr']):
        return 2
    return 0

if __name__ == "__main__":
    import pandas as pd
    import datetime
    df = pd.read_parquet('titanic.parquet')
    (
        df
        .assign(encoded_title=lambda x: [parse_and_encode_title(s) for s in x.Name])       
        [['PassengerId'
          ,'Fare','Pclass'
          ,'encoded_title']]  # restrict to key and two features
        .assign(event_timestamp = datetime.datetime(2023,1,1,0,0,0))  # req'd by feast for slowly change dimension
        .join(pd.get_dummies(df.Sex)) #
        .to_parquet('feast_repo/data/titanic_features.parquet',index=False)
    )
    