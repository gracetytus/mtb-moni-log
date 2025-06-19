import re
import argparse
import gaps_online as go
import matplotlib.pyplot as plt
import polars as pl
import numpy as np
from pathlib import Path
from tqdm import tqdm

def parse_outage_times(file_path):
    outages = []

    # This regex captures lines with: from <start> to <end> with duration <duration>
    pattern = re.compile(r"from (\d+\.\d+) to (\d+\.\d+) with duration")

    with open(file_path, 'r') as f:
        for line in f:
            match = pattern.search(line)
            if match:
                start_time = float(match.group(1))
                end_time = float(match.group(2))
                outages.append((start_time, end_time))
    return outages

def mtb_rate_plot(data : list):
    plt.style.use('publication.rc')
    fig, ax = plt.subplots()
    ax.set_ylabel('Hz', loc='top')
    ax.set_xlabel('unix time (gcu)')

    rates   = np.array([j[1].rate for j in data])
    l_rates = np.array([j[1].lost_rate for j in data])
    times   = np.array([j[0] for j in data])

    ax.plot(times, rates, lw=0.8, alpha=0.7, label='rate')
    ax.plot(times, l_rates, lw=0.8, alpha=0.7, label='lost rate')
    ax.legend(loc='upper right', frameon=False)
    ax.set_title(f'MTB rates', loc='right')
    ax.axvline(start_time, color='black', linestyle='--', lw=1.2, label='gap start')
    ax.axvline(end_time, color='black', linestyle='--', lw=1.2, label='gap end')
    return fig

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Searching for gaps in MTBMoni data packets to indicate MTB outages')
    parser.add_argument('-t', '--telemetry-dir', default='', help='A directory with telemetry binaries, as received from the telemetry stream')
    parser.add_argument('-r', '--report_file', default = '', help = 'the directory with the reports that are output from moni_gaps.py')
    parser.add_argument('-w','--writdir', help='Outdir to save plots', default='')
    args = parser.parse_args()

    # preparing writdir for plots
    outdir = Path(args.writdir or (Path(args.report_file).parent / "plots"))
    outdir.mkdir(parents=True, exist_ok=True)

    outage_unix_time_list = parse_outage_times(args.report_file)
    

    for start_time, end_time in outage_unix_time_list:
        files = go.io.get_telemetry_binaries(start_time -600, end_time -600, data_dir=args.telemetry_dir)
        mtb_moni_series = []

        for f in tqdm(files, desc=f"Processing files from {start_time} to {end_time}"):
            treader = go.io.TelemetryPacketReader(str(f))
            for pack in treader:
                if pack.header.packet_type == go.io.TelemetryPacketType.AnyTofHK:
                    tp = go.io.TofPacket()
                    tp.from_bytestream(pack.payload, 0)

                    if tp.packet_type == go.io.TofPacketType.MonitorMtb:
                        mtb_moni = go.tof.monitoring.MtbMoniData()
                        mtb_moni.from_tofpacket(tp)
                        mtb_moni_series.append((pack.header.gcutime, mtb_moni))

        if mtb_moni_series:
            fig0 = mtb_rate_plot(mtb_moni_series)
            outfile = outdir / f'{int(start_time)}_{int(end_time)}_mtb_rates.pdf'
            fig0.savefig(outfile)
            plt.close(fig0)

