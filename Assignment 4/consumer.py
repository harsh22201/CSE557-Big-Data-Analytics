import time
import pickle
import os
from collections import deque
from kafka import KafkaConsumer
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd

TOPIC = 'wiki-vote'
BOOTSTRAP = 'localhost:9092'
GROUP_ID = 'wiki-consumer'
GRAPH_STATE_FILE = 'graph_state.pkl'

# Ground truth for comparison
GROUND_TRUTH = {
    "Nodes": 7115,
    "Edges": 103689,
    "Nodes in largest WCC": 7066,
    "Edges in largest WCC": 103663,
    "Nodes in largest SCC": 1300,
    "Edges in largest SCC": 39456,
    "Average clustering coefficient": 0.1409,
    "Number of triangles": 608389,
    "Fraction of closed triangles": 0.04564,
    "Diameter": 7,
    "90% effective diameter": 3.8,
}

# ------------------------------------------------------------
# Utility functions for saving/loading
# ------------------------------------------------------------
def save_graph(G):
    pickle.dump(G, open(GRAPH_STATE_FILE, 'wb'))
    print(f"💾 Graph saved ({G.number_of_nodes()} nodes, {G.number_of_edges()} edges).")

def load_graph():
    if os.path.exists(GRAPH_STATE_FILE):
        G = pickle.load(open(GRAPH_STATE_FILE, 'rb'))
        print(f"✅ Loaded graph ({G.number_of_nodes()} nodes, {G.number_of_edges()} edges).")
        return G
    print("ℹ️  Starting new graph.")
    return nx.DiGraph()

# ------------------------------------------------------------
# Main consumer logic
# ------------------------------------------------------------
def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=GROUP_ID
    )

    G = load_graph()
    if(G.number_of_nodes() >= GROUND_TRUTH["Nodes"] and G.number_of_edges() >= GROUND_TRUTH["Edges"]):
        print("⚠️  Graph already complete according to ground truth")
        compute_and_compare_metrics(G, GROUND_TRUTH)
        return

    # Time-series data
    times = [0.0,]
    total_edges_recieved = [0,]
    avg_degrees = [0.0,]

    start_time = -1

    try:
        for message in consumer:
            src, tgt = message.value.decode('utf-8').split(',')
            G.add_edge(src, tgt)
            nodes = G.number_of_nodes()
            edges = G.number_of_edges()

            if start_time == -1:
                start_time = time.time()
                last_time = start_time

            # Update every second
            if time.time() - last_time >= 1:
                elapsed = round(time.time() - start_time, 2)

                # Approximate average degree (2E / N)
                avg_degree = (2 * edges / nodes) if nodes > 0 else 0

                times.append(elapsed)
                total_edges_recieved.append(edges)
                avg_degrees.append(avg_degree)
                print(f"⏱️ Time: {elapsed}s | Nodes: {nodes} | Edges: {edges} | Avg degree: {avg_degree:.2f}")

                last_time = time.time()

            # Stop when complete
            if nodes >= GROUND_TRUTH["Nodes"] and edges >= GROUND_TRUTH["Edges"]:
                elapsed = round(time.time() - start_time, 2)
                avg_degree = (2 * edges / nodes) if nodes > 0 else 0
                times.append(elapsed)
                total_edges_recieved.append(edges)
                avg_degrees.append(avg_degree)
                print(f"⏱️ Time: {elapsed}s | Nodes: {nodes} | Edges: {edges} | Avg degree: {avg_degree:.2f}")
                print("\n✅ Stream reached ground truth metrics!")
                break

    except KeyboardInterrupt:
        print("🛑 Interrupted!")

    finally:
        consumer.close()
        save_graph(G)
        save_time_series_plot(times, total_edges_recieved, avg_degrees)
        compute_and_compare_metrics(G, GROUND_TRUTH)

# ------------------------------------------------------------
# Save combined metrics graph
# ------------------------------------------------------------
def save_time_series_plot(times, edges, avg_degrees):
    if not times:
        print("⚠️ No time-series data to plot.")
        return

    df = pd.DataFrame({
        "time_sec": times,
        "total_edges_recieved": edges,
        "avg_degree": avg_degrees
    })
    df.to_csv("stream_metrics.csv", index=False)
    print("📈  Saved time-series data → stream_metrics.csv")

    # Plot both metrics with twin y-axes
    fig, ax1 = plt.subplots(figsize=(8, 4))
    color1 = 'tab:blue'
    color2 = 'tab:orange'

    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("Total Edges Recieved", color=color1)
    ax1.plot(df["time_sec"], df["total_edges_recieved"], color=color1, label="Total Edges")
    ax1.tick_params(axis='y', labelcolor=color1)

    ax2 = ax1.twinx()
    ax2.set_ylabel("Approx. Avg Degree", color=color2)
    ax2.plot(df["time_sec"], df["avg_degree"], color=color2, linestyle='--', label="Avg Degree")
    ax2.tick_params(axis='y', labelcolor=color2)

    fig.suptitle("Streaming Growth: Total Edges & Approx. Average Degree")
    fig.tight_layout()
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.savefig("stream_growth.png")
    print("🖼️ Saved plot → stream_growth.png")

# ------------------------------------------------------------
# Compute final metrics
# ------------------------------------------------------------
def compute_and_compare_metrics(G, truth):
    print("\n📊 Computing final graph metrics...")
    
    G_undir = G.to_undirected()

    wcc = max(nx.weakly_connected_components(G), key=len)
    scc = max(nx.strongly_connected_components(G), key=len)
    G_wcc = G.subgraph(wcc)
    G_scc = G.subgraph(scc)

    avg_clustering = nx.average_clustering(G_undir)
    num_triangles = sum(nx.triangles(G_undir).values()) // 3
    total_triplets = sum(d * (d - 1) / 2 for _, d in G_undir.degree())
    fraction_closed_triangles = (num_triangles / total_triplets) if total_triplets > 0 else 0
    
    # Compute 90% effective diameter
    distances = dict(nx.all_pairs_shortest_path_length(G_wcc.to_undirected()))
    all_dists = [d for src in distances.values() for d in src.values()]
    all_dists.sort()
    effective_diameter_90 = all_dists[int(0.9 * len(all_dists))]

    metrics = {
        "Nodes": len(G),
        "Edges": G.number_of_edges(),
        "Nodes in largest WCC": len(G_wcc),
        "Edges in largest WCC": G_wcc.number_of_edges(),
        "Nodes in largest SCC": len(G_scc),
        "Edges in largest SCC": G_scc.number_of_edges(),
        "Average clustering coefficient": round(avg_clustering, 4),
        "Number of triangles": num_triangles,
        "Fraction of closed triangles": round(fraction_closed_triangles, 5),
        "Diameter": nx.diameter(G_wcc.to_undirected()),
        "90% effective diameter": round(effective_diameter_90, 2),
    }

    print("\n--- Final Comparison ---")
    print(f"{'Metric':30s} {'Computed':>10s} {'Ground Truth':>15s}")
    for k, v in metrics.items():
        truth_val = truth.get(k, "-")
        print(f"{k:30s} {str(v):>10s} {str(truth_val):>15s}")

# ------------------------------------------------------------
if __name__ == "__main__":
    main()
