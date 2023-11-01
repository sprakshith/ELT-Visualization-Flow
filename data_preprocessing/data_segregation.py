import os
import json
import pandas as pd
from data_extraction.location_extraction import extract_lat_lon, extract_missing_locations

dir_path = os.path.dirname(os.path.realpath(__file__))


def segregate_raw_dataset(fill_missing_locations, fetch_lat_lon):
    file_path = os.path.join(dir_path, "../Datasets/RawDatasets/public_emdat_natural.xlsx")
    df = pd.read_excel(file_path, sheet_name='EM-DAT Data')

    # Creating CSV for the CPI Table
    cpi_list = list()
    for year in df['Start Year'].unique():
        cpi_list.append({
            'CPICode': f'C_{year}',
            'Year': year,
            'Value': df[df['Start Year'] == year]['CPI'].values[0]
        })

    cpi_df = pd.DataFrame(cpi_list)
    cpi_df.sort_values('Year', inplace=True)
    cpi_df.fillna(128.11, inplace=True)

    file_path = os.path.join(dir_path, "../Datasets/CleanedDatasets/CPI.csv")
    cpi_df.to_csv(file_path, sep='|', index=False)

    # Creating CSV for the Country Table
    country_df = df[['ISO', 'Region', 'Subregion', 'Country']].drop_duplicates()
    country_df.columns = ['ISOCode', 'Region', 'Subregion', 'Country']
    country_df.sort_values('ISOCode', inplace=True)
    file_path = os.path.join(dir_path, "../Datasets/CleanedDatasets/Country.csv")
    country_df.to_csv(file_path, sep='|', index=False)

    # Creating CSV for the ExternalReqRes Table
    ext_req_res_list = []
    for ofda in [0, 1]:
        for appeal in [0, 1]:
            for declaration in [0, 1]:
                ext_req_res_dict = {'ExternalReqResId': len(ext_req_res_list) + 1, 'OFDA': ofda, 'Appeal': appeal,
                                    'Declaration': declaration}
                ext_req_res_list.append(ext_req_res_dict)

    external_req_res_df = pd.DataFrame(ext_req_res_list)
    external_req_res_df.sort_values('ExternalReqResId', inplace=True)
    file_path = os.path.join(dir_path, "../Datasets/CleanedDatasets/ExternalReqRes.csv")
    external_req_res_df.to_csv(file_path, sep='|', index=False)

    # Creating CSV for DisasterClassification Table
    disaster_classification_cols = ['Classification Key', 'Disaster Group', 'Disaster Subgroup', 'Disaster Type',
                                    'Disaster Subtype', 'Magnitude Scale']
    disaster_classification = df[disaster_classification_cols].drop_duplicates()
    disaster_classification.columns = ['ClassificationKey', 'Group', 'Subgroup', 'Type', 'Subtype', 'Unit']
    disaster_classification.sort_values('ClassificationKey', inplace=True)
    file_path = os.path.join(dir_path, "../Datasets/CleanedDatasets/DisasterClassification.csv")
    disaster_classification.to_csv(file_path, sep='|', index=False)

    # Creating CSV for AssociateType Table
    df['Associated Types'] = df['Associated Types'].apply(replace_associate_type)

    unique_associated_types = set()
    for types in df['Associated Types'].unique():
        if isinstance(types, str):
            unique_associated_types.update(types.split("|"))

    unique_associated_types = sorted(list(unique_associated_types))

    associate_type_list = [{'AssociateTypeId': index + 1, 'AssociateType': assoc_type}
                           for index, assoc_type in enumerate(unique_associated_types)]
    associate_type_df = pd.DataFrame(associate_type_list)
    associate_type_df.sort_values('AssociateTypeId', inplace=True)
    file_path = os.path.join(dir_path, "../Datasets/CleanedDatasets/AssociateType.csv")
    associate_type_df.to_csv(file_path, sep='|', index=False)

    # Creating CSV for DisasterAssociate Table (Which is a connection between Disaster and AssociateType Table)
    disaster_associate_list = list()
    for associate in associate_type_df.iterrows():
        associate_type_id = associate[1]['AssociateTypeId']
        associate_type = associate[1]['AssociateType']
        for row in df[~df['Associated Types'].isna()].iterrows():
            if associate_type in str(row[1]['Associated Types']):
                disaster_associate_dict = {
                    'DisasterAssociateId': len(disaster_associate_list) + 1,
                    'DisasterNo': row[1]['DisNo.'],
                    'AssociateTypeId': associate_type_id,
                }

                disaster_associate_list.append(disaster_associate_dict)

    disaster_associate = pd.DataFrame(disaster_associate_list)
    file_path = os.path.join(dir_path, "../Datasets/CleanedDatasets/DisasterAssociate.csv")
    disaster_associate.to_csv(file_path, sep='|', index=False)

    # Call OpenAI Function to Extract Missing Location
    if fill_missing_locations:
        extract_missing_locations(df)

    # Adding Newly Found Locations into the df-dataframe.
    file_path = os.path.join(dir_path, "../Datasets/IntermediateDatasets/location_filled.csv")
    location_dataframe = pd.read_csv(file_path, sep="|")

    merged_df = df.merge(location_dataframe[['DisNo.', 'Location']], on='DisNo.', how='left')
    merged_df['Location'] = merged_df['Location_y'].combine_first(merged_df['Location_x'])
    merged_df = merged_df.drop(['Location_x', 'Location_y'], axis=1)

    df = df.merge(merged_df[['DisNo.', 'Location']], on='DisNo.', how='left')
    df['Location'] = df['Location_x'].combine_first(df['Location_y'])
    df = df.drop(['Location_x', 'Location_y'], axis=1)

    # Using Google Geocoding API to find out the Lat and Lon of the Locations
    if fetch_lat_lon:
        extract_lat_lon(df)

    # Creating CSV for AssociateType Table
    location_details_list = list()
    for row in df[~df.Location.isna()].iterrows():
        dis_no = row[1]['DisNo.']
        file_path = os.path.join(dir_path, f'../Datasets/IntermediateDatasets/GeocodedJsonFiles/{dis_no}.json')
        geo_coding = json.loads(open(file_path).read())

        for loc in geo_coding:
            location_dict = {
                'LocationID': len(location_details_list) + 1,
                'DisasterNo': dis_no,
                'Location': loc['formatted_address'],
                'Latitude': loc['geometry']['location']['lat'],
                'Longitude': loc['geometry']['location']['lng']
            }

            location_details_list.append(location_dict)

    location_details_dataframe = pd.DataFrame(location_details_list)
    file_path = os.path.join(dir_path, f'../Datasets/CleanedDatasets/Location.csv')
    location_details_dataframe.to_csv(file_path, sep='|', index=False)

    # Creating the CSV for Disaster Table (This is a fact table).
    disaster_list = list()
    for row in df.iterrows():
        disaster_list.append({
            'DisasterId': len(disaster_list) + 1,
            'DisasterNum': row[1]['DisNo.'],
            'ClassificationKey': row[1]['Classification Key'],
            'ISOCode': row[1]['ISO'],
            'ExternalReqResId': fetch_ext_req_res_code(row, external_req_res_df),
            'EventName': row[1]['Event Name'],
            'RiverBasin': row[1]['River Basin'],
            'DisasterOrigin': row[1]['Origin'],
            'DisasterMagnitude': row[1]['Magnitude'],
            'AidContribution': row[1]['AID Contribution (\'000 US$)'],
            'StartYear': row[1]['Start Year'],
            'StartMonth': row[1]['Start Month'],
            'StartDay': row[1]['Start Day'],
            'EndYear': row[1]['End Year'],
            'EndMonth': row[1]['End Month'],
            'EndDay': row[1]['End Day'],
            'TotalDeaths': row[1]['Total Deaths'],
            'NumInjured': row[1]['No. Injured'],
            'NumAffected': row[1]['No. Affected'],
            'NumHomeless': row[1]['No. Homeless'],
            'TotalAffected': row[1]['Total Affected'],
            'ReconstructionCost': row[1]['Reconstruction Costs (\'000 US$)'],
            'ReconstructionCostAdj': row[1]['Reconstruction Costs, Adjusted (\'000 US$)'],
            'InsuredDamage': row[1]['Insured Damage (\'000 US$)'],
            'InsuredDamageAdj': row[1]['Insured Damage, Adjusted (\'000 US$)'],
            'TotalDamage': row[1]['Total Damage (\'000 US$)'],
            'TotalDamageAdj': row[1]['Total Damage, Adjusted (\'000 US$)'],
            'CPICode': f'C_{row[1]["Start Year"]}',
            'EntryDate': row[1]['Entry Date'],
            'UpdatedDate': row[1]['Last Update']
        })

    disaster_df = pd.DataFrame(disaster_list)
    file_path = os.path.join(dir_path, f'../Datasets/CleanedDatasets/Disaster.csv')
    disaster_df.to_csv(file_path, sep='|', index=False)


def replace_associate_type(assoc_types):
    if not isinstance(assoc_types, str):
        return assoc_types

    replacements = {
        'Snow/ice': 'Snow',
        'Avalanche (Snow, Debris)': 'Avalanche',
        'Broken Dam/Burst bank': 'Burst dam or bank',
        'Tsunami/Tidal wave': 'Tidal wave',
        'Slide (land, mud, snow, rock)': 'Land slide',
    }

    for old, new in replacements.items():
        assoc_types = assoc_types.replace(old, new)

    return assoc_types


def fetch_ext_req_res_code(row, ext_req_res_df):
    ofda_con = ext_req_res_df['OFDA'] == (row[1]['OFDA Response'] == 'Yes')
    appeal_con = ext_req_res_df['Appeal'] == (row[1]['Appeal'] == 'Yes')
    declaration_con = ext_req_res_df['Declaration'] == (row[1]['Declaration'] == 'Yes')
    ext_req_res_row = ext_req_res_df[ofda_con & appeal_con & declaration_con]

    return ext_req_res_row['ExternalReqResId'].values[0]
