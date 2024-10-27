from pymongo import MongoClient
from datetime import datetime
from bson.son import SON

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['village']

# Collections
users_col = db['users']
water_consumption_col = db['water_consumption']
registered_wells_col = db['registered_wells']
control_activities_col = db['control_activities']

def check_last_status_change_dates():
    print("\nChecking that 'last_status_change' dates are after 'authorization_date'...")
    query = {
        '$expr': {
            '$and': [
                { '$ne': ["$authorization_date", None] },
                { '$lt': ["$last_status_change", "$authorization_date"] }
            ]
        }
    }
    wells = list(registered_wells_col.find(query))
    if not wells:
        print("PASS: All 'last_status_change' dates are after 'authorization_date'.")
    else:
        print("FAIL: Found wells where 'last_status_change' is before 'authorization_date':")
        for well in wells:
            print(f" - Well ID: {well['well_id']}, Authorization Date: {well['authorization_date']}, Last Status Change: {well['last_status_change']}")

def check_control_activities_coverage():
    print("\nChecking control activities coverage (should be ~90%)...")
    total_legal_wells = registered_wells_col.count_documents({ 'authorization_date': { '$ne': None } })
    wells_with_activities = len(control_activities_col.distinct('well_id'))
    coverage_percentage = (wells_with_activities / total_legal_wells) * 100 if total_legal_wells else 0
    print(f"Total legal wells: {total_legal_wells}")
    print(f"Wells with control activities: {wells_with_activities}")
    print(f"Control Activities Coverage: {coverage_percentage:.2f}%")
    if 85 <= coverage_percentage <= 95:
        print("PASS: Control activities coverage is within expected range.")
    else:
        print("WARN: Control activities coverage is outside expected range.")

def check_no_control_activities_for_illegal_well():
    print("\nChecking that there are no control activities for the illegal well...")
    illegal_well = registered_wells_col.find_one({ 'authorization_date': None })
    if not illegal_well:
        print("WARN: No illegal well found (no well without 'authorization_date').")
        return
    activities = list(control_activities_col.find({ 'well_id': illegal_well['well_id'] }))
    if not activities:
        print("PASS: No control activities found for the illegal well.")
    else:
        print("FAIL: Found control activities for the illegal well:")
        for activity in activities:
            print(f" - Control ID: {activity['control_id']}, Date: {activity['date']}")

def check_control_activity_dates():
    print("\nChecking that control activity dates are after 'last_status_change' and before today...")
    pipeline = [
        {
            '$lookup': {
                'from': "registered_wells",
                'localField': "well_id",
                'foreignField': "well_id",
                'as': "well_info"
            }
        },
        { '$unwind': "$well_info" },
        {
            '$project': {
                'control_id': 1,
                'date': 1,
                'well_id': 1,
                'last_status_change': "$well_info.last_status_change",
                'dateBeforeLastStatusChange': {
                    '$lt': ["$date", "$well_info.last_status_change"]
                }
            }
        },
        { '$match': { 'dateBeforeLastStatusChange': True } }
    ]
    issues = list(control_activities_col.aggregate(pipeline))
    if not issues:
        print("PASS: All control activity dates are after 'last_status_change' of the wells.")
    else:
        print("FAIL: Found control activities with dates before 'last_status_change':")
        for issue in issues:
            print(f" - Control ID: {issue['control_id']}, Control Date: {issue['date']}, Last Status Change: {issue['last_status_change']}")

def check_future_user_registration_dates():
    print("\nChecking for users with 'registration_date' in the future...")
    future_users = list(users_col.find({ 'registration_date': { '$gt': datetime.now() } }))
    if not future_users:
        print("PASS: No users have 'registration_date' in the future.")
    else:
        print("FAIL: Found users with 'registration_date' in the future:")
        for user in future_users:
            print(f" - User ID: {user['user_id']}, Registration Date: {user['registration_date']}")

def identify_illegal_well_and_owner():
    print("\nIdentifying the illegal well and its owner...")
    # Step a: Identify high consumption users
    high_consumption_users = water_consumption_col.aggregate([
        {
            '$group': {
                '_id': "$user_id",
                'totalConsumption': { '$sum': "$consumption_m3" }
            }
        },
        { '$sort': { 'totalConsumption': -1 } },
        { '$limit': 10 }  # Adjust as needed
    ])
    high_consumption_user_ids = [u['_id'] for u in high_consumption_users]

    # Step b: Find suspect wells
    suspect_wells = list(registered_wells_col.find({
        'owner_user_id': { '$in': high_consumption_user_ids },
        '$or': [
            { 'status': "Inactive" },
            { 'authorization_date': None }
        ]
    }))
    if not suspect_wells:
        print("No suspect wells found among high consumption users.")
        return
    suspect_well_ids = [w['well_id'] for w in suspect_wells]

    # Step c: Filter wells with no control activities
    wells_with_no_control_activities = []
    for well_id in suspect_well_ids:
        activity = control_activities_col.find_one({ 'well_id': well_id })
        if not activity:
            wells_with_no_control_activities.append(well_id)

    # Step d: Identify the illegal well and user
    if wells_with_no_control_activities:
        illegal_well_id = wells_with_no_control_activities[0]
        illegal_well = registered_wells_col.find_one({ 'well_id': illegal_well_id })
        illegal_user = users_col.find_one({ 'user_id': illegal_well['owner_user_id'] })

        print("\n=== Illegal Well and Owner Identified ===")
        print(f"Illegal Well ID: {illegal_well['well_id']}")
        print(f"Owner User ID: {illegal_user['user_id']}")
        print(f"Owner Name: {illegal_user['name']}")
        print(f"Owner Address: {illegal_user['address']}")
    else:
        print("No illegal well found among suspect wells.")

def check_dates_in_future_in_registered_wells():
    print("\nChecking for wells with 'last_status_change' or 'authorization_date' in the future...")
    wells_in_future = list(registered_wells_col.find({
        '$or': [
            { 'last_status_change': { '$gt': datetime.now() } },
            { 'authorization_date': { '$gt': datetime.now() } }
        ]
    }))
    if not wells_in_future:
        print("PASS: No wells have dates in the future.")
    else:
        print("FAIL: Found wells with dates in the future:")
        for well in wells_in_future:
            print(f" - Well ID: {well['well_id']}, Authorization Date: {well['authorization_date']}, Last Status Change: {well['last_status_change']}")

def extract_first_syllable(name):
    # Split the full name into parts (first name and surnames)
    name_parts = name.split()

    # Helper function to extract the first syllable of a name part
    def get_syllable(part):
        if len(part) < 3:
            return part.upper()
        return part[:3].upper()

    # Extract first syllable from each part of the name (first name and surnames)
    first_syllables = [get_syllable(part) for part in name_parts]

    # Join the syllables into a single string separated by a space
    return ''.join(first_syllables)

def check_names_length():
    print("\nChecking for users with names shorter than 3 characters...")
    short_names = list(users_col.find({ '$expr': { '$lt': [{'$strLenCP': "$name"}, 3] } }))
    if not short_names:
        print("PASS: No users have names shorter than 3 characters.")
    else:
        print("FAIL: Found users with names shorter than 3 characters:")
        for user in short_names:
            print(f" - User ID: {user['user_id']}, Name: {user['name']}")

def identify_illegal_well_and_owner():
    print("\nIdentifying the illegal well and its owner...")
    # Step a: Identify high consumption users
    high_consumption_users = water_consumption_col.aggregate([
        {
            '$group': {
                '_id': "$user_id",
                'totalConsumption': { '$sum': "$consumption_m3" }
            }
        },
        { '$sort': { 'totalConsumption': -1 } },
        { '$limit': 10 }  # Adjust as needed
    ])
    high_consumption_user_ids = [u['_id'] for u in high_consumption_users]

    # Step b: Find suspect wells
    suspect_wells = list(registered_wells_col.find({
        'owner_user_id': { '$in': high_consumption_user_ids },
        '$or': [
            { 'status': "Inactive" },
            { 'authorization_date': None }
        ]
    }))
    if not suspect_wells:
        print("No suspect wells found among high consumption users.")
        return
    suspect_well_ids = [w['well_id'] for w in suspect_wells]

    # Step c: Filter wells with no control activities
    wells_with_no_control_activities = []
    for well_id in suspect_well_ids:
        activity = control_activities_col.find_one({ 'well_id': well_id })
        if not activity:
            wells_with_no_control_activities.append(well_id)

    # Step d: Identify the illegal well and user
    if wells_with_no_control_activities:
        illegal_well_id = wells_with_no_control_activities[0]
        illegal_well = registered_wells_col.find_one({ 'well_id': illegal_well_id })
        illegal_user = users_col.find_one({ 'user_id': illegal_well['owner_user_id'] })

        first_syllable = extract_first_syllable(illegal_user['name'])

        print("\n=== Illegal Well and Owner Identified ===")
        print(f"Illegal Well ID: {illegal_well['well_id']}")
        print(f"Owner User ID: {illegal_user['user_id']}")
        print(f"Owner Name: {illegal_user['name']}")
        print(f"Owner Address: {illegal_user['address']}")
        print(f"Solution (First syllable of owner name in caps): {first_syllable}")
    else:
        print("No illegal well found among suspect wells.")

def main():
    print("Starting data validation and analysis...\n")

    check_last_status_change_dates()
    check_control_activities_coverage()
    check_no_control_activities_for_illegal_well()
    check_control_activity_dates()
    check_future_user_registration_dates()
    check_dates_in_future_in_registered_wells()
    check_names_length()  # New validation step for name length
    identify_illegal_well_and_owner()

    print("\nData validation and analysis complete.")

if __name__ == '__main__':
    main()