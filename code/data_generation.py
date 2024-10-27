from pymongo import MongoClient
from faker import Faker
import random
import uuid

from datetime import datetime, timedelta

# Initialize Faker
fake = Faker('es_ES')  # Use 'es_ES' for Spain or 'es_MX' for Mexico

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['village']

# Clear existing collections if any
db.users.drop()
db.water_consumption.drop()
db.registered_wells.drop()
db.control_activities.drop()
db.demography.drop()

# Collections
users_col = db['users']
water_consumption_col = db['water_consumption']
registered_wells_col = db['registered_wells']
control_activities_col = db['control_activities']
demography_col = db['demography']

# Helper function to convert date to datetime


def date_to_datetime(d):
    return datetime.combine(d, datetime.min.time())

# 1. Generate Users Data


def generate_users(num_users):
    users = []
    for user_id in range(1, num_users + 1):
        registration_date = fake.date_between(
            start_date='-5y', end_date='today')
        user = {
            'user_id': str(uuid.uuid4()),
            'name': fake.name(),
            'address': fake.address().split('\n')[0].rstrip(),
            'registration_date': date_to_datetime(registration_date)
        }
        users.append(user)
    users_col.insert_many(users)
    return users

def generate_registered_wells(users):
    wells = []
    illegal_user = random.choice(users)  # Select one user to be the illegal user
    illegal_well_id = str(uuid.uuid4())  # Generate a unique well_id for the illegal well

    max_lat, max_long = 42.822458, -5.593844
    min_lat, min_long = 42.788357, -5.669483

    for user in users:

        if user == illegal_user:  # Create the illegal well
            illegal_well = {
                'well_id': illegal_well_id,
                'location': {
                    'latitude': float(random.uniform(min_lat, max_lat)),   # Convert to float
                    'longitude': float(random.uniform(min_long, max_long))  # Convert to float
                },
                'owner_user_id': illegal_user['user_id'],
                'authorization_date': None,  # No authorization for illegal wells
                'status': 'Inactive',
                'last_status_change': None  # No status change date for illegal wells
            }
            wells.append(illegal_well)
        else:
            num_wells_for_user = random.randint(1, 10)  # Randomly assign between 1 and 10 wells per user
            for _ in range(num_wells_for_user):
                # Decide whether the authorization_date should be quite old
                if random.random() < 0.5:  # 50% chance
                    # authorization_date between '-30y' and '-5y'
                    authorization_date = datetime.combine(fake.date_between(start_date='-30y', end_date='-5y'), datetime.min.time())
                else:
                    # authorization_date between '-5y' and 'today'
                    authorization_date = datetime.combine(fake.date_between(start_date='-5y', end_date='today'), datetime.min.time())
                # Generate last_status_change date after authorization_date and before today
                delta_days = (datetime.now() - authorization_date).days
                if delta_days > 0:
                    last_status_change_date = authorization_date + timedelta(days=random.randint(1, delta_days))
                else:
                    last_status_change_date = authorization_date

                well = {
                    'well_id': str(uuid.uuid4()),
                    'location': {
                        'latitude': float(random.uniform(min_lat, max_lat)),   # Convert to float
                        'longitude': float(random.uniform(min_long, max_long))  # Convert to float
                    },
                    'owner_user_id': user['user_id'],
                    'authorization_date': authorization_date,
                    'status': random.choice(['Active', 'Inactive']),
                    'last_status_change': last_status_change_date
                }
                wells.append(well)
    registered_wells_col.insert_many(wells)
    return wells, illegal_user, illegal_well  # Return the illegal_well too

# 2. Generate Water Consumption Data (with abnormal consumption for the illegal user)


def generate_water_consumption(users, illegal_user, num_records_per_user=50):
    consumption_records = []
    for user in users:
        previous_reading = random.uniform(0, 100)
        date = datetime.now() - timedelta(days=365)

        # If the user is the illegal user, give them abnormally high consumption
        is_illegal_user = user['user_id'] == illegal_user['user_id']

        for _ in range(num_records_per_user):
            # For the illegal user, increase variation to simulate higher usage
            consumption_increase = random.uniform(
                0, 10) if not is_illegal_user else random.uniform(20, 50)
            current_reading = previous_reading + consumption_increase
            consumption = {
                'user_id': user['user_id'],
                'date': date,
                'consumption_m3': current_reading - previous_reading,
                'previous_reading': previous_reading,
                'current_reading': current_reading,
                'variation': current_reading - previous_reading
            }
            consumption_records.append(consumption)
            previous_reading = current_reading
            date += timedelta(days=7)  # Weekly readings
    water_consumption_col.insert_many(consumption_records)
    return consumption_records


def generate_control_activities(registered_wells, illegal_well):
    activities = []
    # Filter out the illegal well and get 90% of the remaining wells
    legal_wells = [well for well in registered_wells if well['well_id'] != illegal_well['well_id']]
    num_activities = int(0.9 * len(legal_wells))  # 90% of legal wells

    # Randomly choose 90% of the legal wells
    selected_wells = random.sample(legal_wells, num_activities)

    for well in selected_wells:
        # Ensure control activity date is after last_status_change and before today
        start_date = well['last_status_change']
        end_date = datetime.now()
        delta_days = (end_date - start_date).days
        if delta_days > 0:
            control_activity_date = start_date + timedelta(days=random.randint(1, delta_days))
        else:
            control_activity_date = start_date

        activity = {
            'control_id': str(uuid.uuid4()),
            'date': control_activity_date,
            'control_type': random.choice(['Inspection', 'Verification', 'Audit']),
            'result': random.choice(['Legal', 'No anomalies']),
            'observations': fake.sentence(nb_words=10),
            'well_id': well['well_id']
        }
        activities.append(activity)

    control_activities_col.insert_many(activities)
    return activities


# Main execution
if __name__ == '__main__':
    print("Generating Users...")
    users = generate_users(1000)

    print("Generating Registered Wells Data...")
    registered_wells, illegal_user, illegal_well = generate_registered_wells(users)  # Get the illegal well

    print("Generating Water Consumption Data...")
    water_consumption = generate_water_consumption(users, illegal_user)

    print("Generating Control Activities Data...")
    control_activities = generate_control_activities(registered_wells, illegal_well)  # Pass illegal well here

    print("Data generation complete.")
