from flask import Flask, render_template, jsonify, Response
import io
import happybase
import pandas as pd
import matplotlib.pyplot as plt

app = Flask(__name__)

# Connect to HBase and fetch data
def get_hbase_data():
    connection = happybase.Connection('localhost')
    connection.open()
    table = connection.table('ecommerce_orders')
    rows = table.scan()

    # Extract the data in a structured format
    data = []
    for key, row in rows:
        row_data = {
            'order_details:amount': float(row.get(b'order_details:amount', [None])[0]) if row.get(b'order_details:amount') else None,
            'order_details:order_id': int(row.get(b'order_details:order_id', [None])[0]) if row.get(b'order_details:order_id') else None,
            'order_details:user_id': int(row.get(b'order_details:user_id', [None])[0]) if row.get(b'order_details:user_id') else None,
            'order_details:category': decode_value(row.get(b'order_details:category')),
            'payment_info:payment_method': decode_value(row.get(b'payment_info:payment_method')),
            'status_info:status': decode_value(row.get(b'status_info:status')),
            'timestamp_info:timestamp': decode_value(row.get(b'timestamp_info:timestamp'))
        }
        data.append(row_data)
    
    connection.close()
    return data

def decode_value(value):
    if value and isinstance(value, bytes):
        return value.decode('utf-8')
    return value

def get_aggregations(data):
    df = pd.DataFrame(data)

    total_cost_per_category = df.groupby('order_details:category')['order_details:amount'].sum().reset_index().values.tolist()
    status_count = df['status_info:status'].value_counts().to_dict()
    status_count_per_category = {f"{category}-{status}": count for (category, status), count in df.groupby('order_details:category')['status_info:status'].value_counts().to_dict().items()}
    avg_cost_per_category = df.groupby('order_details:category')['order_details:amount'].mean().reset_index().values.tolist()
    top_users = (df.groupby('order_details:user_id')['order_details:amount'].sum()
                   .sort_values(ascending=False).head(10).reset_index()
                   .rename(columns={'order_details:user_id': 'user_id', 'order_details:amount': 'total_cost'}).to_dict(orient='records'))

    total_amount_per_payment_method = df.groupby('payment_info:payment_method')['order_details:amount'].sum().reset_index().values.tolist()

    return total_cost_per_category, status_count, status_count_per_category, avg_cost_per_category, top_users, total_amount_per_payment_method

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/graph/total_cost_per_category')
def graph_total_cost_per_category():
    data = get_hbase_data()
    total_cost_per_category, _, _, _, _, _ = get_aggregations(data)
    categories = [item[0] for item in total_cost_per_category]
    values = [item[1] for item in total_cost_per_category]

    fig, ax = plt.subplots()
    ax.bar(categories, values)
    ax.set_title('Total Cost Per Category')
    ax.set_xlabel('Category')
    ax.set_ylabel('Total Cost')
    plt.xticks(rotation=45)
    return generate_graph(fig)

@app.route('/graph/average_cost_per_category')
def graph_average_cost_per_category():
    data = get_hbase_data()
    _, _, _, avg_cost_per_category, _, _ = get_aggregations(data)
    categories = [item[0] for item in avg_cost_per_category]
    values = [item[1] for item in avg_cost_per_category]

    fig, ax = plt.subplots()
    ax.bar(categories, values, color='orange')
    ax.set_title('Average Cost Per Category')
    ax.set_xlabel('Category')
    ax.set_ylabel('Average Cost')
    plt.xticks(rotation=45)
    return generate_graph(fig)

@app.route('/graph/status_count')
def graph_status_count():
    data = get_hbase_data()
    _, status_count, _, _, _, _ = get_aggregations(data)
    statuses = list(status_count.keys())
    counts = list(status_count.values())

    fig, ax = plt.subplots()
    ax.pie(counts, labels=statuses, autopct='%1.1f%%', startangle=140)
    ax.set_title('Order Status Distribution')
    return generate_graph(fig)

@app.route('/graph/top_10_buying_users')
def graph_top_10_buying_users():
    data = get_hbase_data()
    _, _, _, _, top_users, _ = get_aggregations(data)
    user_ids = [user['user_id'] for user in top_users]
    total_costs = [user['total_cost'] for user in top_users]

    fig, ax = plt.subplots()
    ax.bar(user_ids, total_costs, color='green')
    ax.set_title('Top 10 Buying Users')
    ax.set_xlabel('User ID')
    ax.set_ylabel('Total Cost')
    plt.xticks(rotation=45)
    return generate_graph(fig)

@app.route('/graph/total_amount_per_payment_method')
def graph_total_amount_per_payment_method():
    data = get_hbase_data()
    _, _, _, _, _, total_amount_per_payment_method = get_aggregations(data)
    payment_methods = [item[0] for item in total_amount_per_payment_method]
    amounts = [item[1] for item in total_amount_per_payment_method]

    fig, ax = plt.subplots()
    ax.bar(payment_methods, amounts, color='blue')
    ax.set_title('Total Amount Per Payment Method')
    ax.set_xlabel('Payment Method')
    ax.set_ylabel('Total Amount')
    plt.xticks(rotation=45)
    return generate_graph(fig)

# Function to generate a graph and return it as a response
def generate_graph(figure):
    img = io.BytesIO()
    figure.savefig(img, format='png')
    img.seek(0)
    return Response(img.getvalue(), mimetype='image/png')

if __name__ == '__main__':
    app.run(debug=True)
