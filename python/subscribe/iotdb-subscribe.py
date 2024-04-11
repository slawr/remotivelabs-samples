from __future__ import annotations

import argparse
import time
from typing import Optional
from remotivelabs.broker.sync import (
    Client,
    SignalsInFrame,
    BrokerException,
    SignalIdentifier,
)
from iotdb.Session import Session
import datetime

# Simple Apache IoTDB session configuration
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="GMT+00:00")
device_id_ = "root.test2.dev1"

def iotdb_open_session():
    session.open(False)

def iotdb_close_session():
    session.close()

def run_subscribe_sample(url: str, signals: list[str], secret: Optional[str] = None):
    client = Client(client_id="Sample client")
    client.connect(url=url, api_key=secret)

    iotdb_open_session()

    def on_signals(signals_in_frame: SignalsInFrame):
        for signal in signals_in_frame:
            ts_ = int(signal.timestamp_us()/1000)
            vss_name_ = signal.name()
            iotdb_vss_name_ = ['`{}`'.format(vss_name_)]
            vss_value_ = [str(signal.value())]
            print(f'TS={ts_} iotdb_name={iotdb_vss_name_} name={vss_name_} value={vss_value_}') 
            # IoTDB does data type inference for basic types based on the timeseries schema
            session.insert_str_record(device_id_, ts_, iotdb_vss_name_, vss_value_)
            print(signal.to_json())

    client.on_signals = on_signals

    try:

        def to_signal_id(signal: str):
            s = signal.split(":")
            if len(s) != 2:
                print("--signals must be in format namespace:signal_name")
                exit(1)
            return SignalIdentifier(s[1], s[0])

        subscription = client.subscribe(
            signals_to_subscribe_to=list(map(to_signal_id, signals)),
            changed_values_only=False,
        )
    except BrokerException as e:
        print(e)
        iotdb_close_session()
        exit(1)
    except Exception as e:
        print(e)
        iotdb_close_session()
        exit(1)

    try:
        print(
            "Broker connection and subscription setup completed, waiting for signals..."
        )
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        subscription.cancel()
        print("Keyboard interrupt received, closing")
        iotdb_close_session()


def main():
    parser = argparse.ArgumentParser(description="Provide address to RemotiveBroker")

    parser.add_argument(
        "-u",
        "--url",
        help="URL of the RemotiveBroker",
        type=str,
        required=False,
        default="http://127.0.0.1:50051",
    )

    parser.add_argument(
        "-x",
        "--x_api_key",
        help="API key is required when accessing brokers running in the cloud",
        type=str,
        required=False,
        default=None,
    )

    parser.add_argument(
        "-t",
        "--access_token",
        help="Personal or service-account access token",
        type=str,
        required=False,
        default=None,
    )

    parser.add_argument(
        "-s", "--signals", help="Signal to subscribe to", required=True, nargs="*"
    )

    try:
        args = parser.parse_args()
    except Exception as e:
        return print("Error specifying signals to use:", e)

    if len(args.signals) == 0:
        print("You must subscribe to at least one signal with --signals namespace:somesignal")
        exit(1)

    secret = args.x_api_key if args.x_api_key is not None else args.access_token
    run_subscribe_sample(args.url, args.signals, secret)


if __name__ == "__main__":
    main()
