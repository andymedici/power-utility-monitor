from app import app, PowerUtilityMonitor

with app.app_context():
    monitor = PowerUtilityMonitor()
    result = monitor.run_monitoring_cycle()
    print(f"Monitoring complete: {result['requests_stored']} new requests stored")