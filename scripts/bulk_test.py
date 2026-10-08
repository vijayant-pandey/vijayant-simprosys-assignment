import concurrent.futures
import subprocess


def send_order(shop_number, order_number):
    event_id = f"load_shop{shop_number:03d}_order{order_number}"
    shop_id = f"shop_{shop_number:03d}"

    command = [
        "python",
        "scripts\\send_webhook.py",
        "--event-id",
        event_id,
        "--shop-id",
        shop_id,
        "--amount",
        "1000",
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    return event_id, result.stdout.strip()


def main():
    jobs = []

    # 5 shops × 10 orders = 50 webhook requests
    for shop in range(1, 6):
        for order in range(1, 11):
            jobs.append((shop, order))

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(
            lambda job: send_order(*job),
            jobs,
        )

        for event_id, output in results:
            print(event_id, output)


if __name__ == "__main__":
    main()