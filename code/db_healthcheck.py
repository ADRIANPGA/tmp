import pymongo

def check_repl_initialized(name, host, port):
    try:
        client = pymongo.MongoClient(host=host, port=port, serverSelectionTimeoutMS=2000)
        result = client.admin.command('replSetGetStatus')
        if result.get('ok') == 1:
            return True
        else:
            return False
    except Exception:
        return False

def check_shards_added(host, port):
    try:
        client = pymongo.MongoClient(host=host, port=port, serverSelectionTimeoutMS=2000)
        result = client.admin.command('listShards')
        shards = result.get('shards', [])
        if len(shards) >= 3:
            return True
        else:
            return False
    except Exception:
        return False

def main():
    services = [
        {'name': 'Config Server Replica Set', 'host': 'mongo-config', 'port': 27017},
        {'name': 'Shard Alpha Replica Set', 'host': 'shard-alpha', 'port': 27018},
        {'name': 'Shard Beta Replica Set', 'host': 'shard-beta', 'port': 27019},
        {'name': 'Shard Charlie Replica Set', 'host': 'shard-charlie', 'port': 27020},
    ]
    
    results = []
    
    for service in services:
        initialized = check_repl_initialized(service['name'], service['host'], service['port'])
        results.append({'name': service['name'], 'status': initialized})
        
    # Check if shards are added to mongos
    shards_added = check_shards_added('mongos', 27017)
    
    # Output the results
    print("Cluster Setup Checklist:")
    for result in results:
        status = '[✓]' if result['status'] else '[x]'
        print(f"{status} {result['name']} initialized")
        
    status = '[✓]' if shards_added else '[x]'
    print(f"{status} Shards added to the cluster")
    
if __name__ == '__main__':
    main()
