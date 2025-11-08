from kafka import KafkaProducer
import time
import argparse
import random

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--file', default='wiki-Vote.txt', help='path to wiki-Vote file')
    p.add_argument('--bootstrap', default='localhost:9092', help='bootstrap server')
    p.add_argument('--delay', type=float, default=0.01, help='seconds between messages')
    p.add_argument('--batch', type=int, default=1, help='send this many messages then sleep delay')
    p.add_argument('--shuffle', action='store_true', help='shuffle edges before sending')
    return p.parse_args()

def read_edges(path):
    edges = []
    with open(path, 'r') as f:
        for line in f:
            if line.startswith('#') or not line.strip():
                continue
            parts = line.strip().split()
            if len(parts) < 2:
                continue
            edges.append((parts[0], parts[1]))
    return edges

if __name__ == "__main__":
    args = parse_args()
    edges = read_edges(args.file)
    
    if(args.shuffle):
        random.shuffle(edges)
    
    producer = KafkaProducer(bootstrap_servers=[args.bootstrap])
    
    count = 0
    try:
        for src, dst in edges:
            producer.send('wiki-vote', f"{src},{dst}".encode('utf-8'))
            count += 1
            if args.batch <= 1:
                time.sleep(args.delay)
            else:
                if count % args.batch == 0:
                    time.sleep(args.delay)
            if count % 10000 == 0:
                print(f"Produced {count} edges")
        producer.flush()
        print("Done producing all edges.")
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        producer.close()
