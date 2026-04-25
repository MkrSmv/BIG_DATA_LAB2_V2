import csv
import random

def generate_iot_telemetry(num_samples=500000, output_path="iot_telemetry.csv"):
    random.seed(42)
    print(f"Generating {num_samples} samples of IoT telemetry...")
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["temperature", "vibration", "pressure", "voltage", "is_anomaly"])
        
        for i in range(num_samples):
            # 15% probability of anomaly
            is_anomaly_val = 1 if random.random() < 0.15 else 0
            
            # Base normal values
            temp = random.gauss(45.0, 5.0)
            vibration = random.gauss(10.0, 2.0)
            pressure = random.gauss(100.0, 10.0)
            voltage = random.gauss(220.0, 5.0)
            
            # Add anomaly deviations
            if is_anomaly_val == 1:
                temp += random.gauss(15.0, 5.0)
                vibration += random.gauss(15.0, 5.0)
                voltage -= random.gauss(20.0, 10.0)
                pressure += random.gauss(30.0, 15.0)
                
            writer.writerow([
                round(temp, 2),
                round(vibration, 2),
                round(pressure, 2),
                round(voltage, 2),
                is_anomaly_val
            ])
            
            if (i + 1) % 100000 == 0:
                print(f"Generated {i + 1} rows...")

    print(f"Dataset generated at {output_path} successfully.")

if __name__ == "__main__":
    generate_iot_telemetry(500000, "iot_telemetry.csv")
