import gaps_online as go
import numpy as np
from tqdm import tqdm
import argparse
import matplotlib.pyplot as plt
from datetime import datetime, UTC
import gc

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Searching for gaps in MTBMoni data packets to indicate MTB outages')
    parser.add_argument('--telemetry-dir', default='', help='A directory with telemetry binaries, as received from the telemetry stream')
    parser.add_argument('-s','--start-time', type=int, default=-1, help='The run start time, e.g. as taken from the elog')
    parser.add_argument('-e','--end-time',type=int, default=-1, help='The run end time, e.g. as taken from the elog')
    parser.add_argument('-w', '--window', type = int, help = 'the window for which missing MTBMoni packets would constitute an outage')

    args = parser.parse_args()

    files = go.io.get_telemetry_binaries(args.start_time, args.end_time, data_dir=args.telemetry_dir)
    packet_ts = []
    moni_gaps = []

    for f in tqdm(files, desc='Reading files..'):
        treader = go.io.TelemetryPacketReader(str(f))
        for packet in treader:
            gcu = packet.header.gcutime
            if int(packet.header.packet_type) == 92:
                tp = go.io.TofPacket()
                tp.from_bytestream(packet.payload, 0)
                if int(tp.packet_type) == 90:

                    moni = go.tof.monitoring.MtbMoniData()
                    moni.from_tofpacket(tp)
                    tiu_busy = moni.tiu_busy_len
                    daq_queue = moni.daq_queue_len
                    temp = moni.fpga_temp
                    rate = moni.rate
                    lost_rate = moni.lost_rate
                    aux = moni.vccaux
                    bram = moni.vccbram
                    vint = moni.vccint

                    packet_ts.append((gcu, tiu_busy, daq_queue, temp, rate, lost_rate, aux, bram, vint)) 

    packet_ts.sort()
    hdurations = []

    dt_start = datetime.fromtimestamp(args.start_time, UTC)
    dt_end = datetime.fromtimestamp(args.end_time, UTC)
    dt1 = datetime.fromtimestamp(packet_ts[0][0], UTC)
    dt2 = datetime.fromtimestamp(packet_ts[-1][0], UTC)
    
    #print(f'the start time was {dt_start.strftime('%Y-%m-%d %H:%M:%S UTC')} and the first MTBMoniData was received {dt1.strftime('%Y-%m-%d %H:%M:%S UTC')}')
    #print(f'the end time was {dt_end.strftime('%Y-%m-%d %H:%M:%S UTC')} and the last MTBMoniData was received {dt2.strftime('%Y-%m-%d %H:%M:%S UTC')}')

    for i in range(1, len(packet_ts)):
        start = packet_ts[i-1][0]
        end = packet_ts[i][0]
        duration = end - start
        hdurations.append(duration)

        tiu = packet_ts[i-1][1]
        daq = packet_ts[i-1][2]
        t = packet_ts[i-1][3]
        r = packet_ts[i-1][4]
        lr = packet_ts[i-1][5]
        vaux = packet_ts[i-1][6]
        vbram = packet_ts[i-1][7]
        vvint = packet_ts[i-1][8]

        if duration >= args.window:
            moni_gaps.append((start, end, duration, tiu, daq, t, r, lr, vaux, vbram, vvint)) 

        
    packet_ts.clear
    gc.collect()
    
    for i in range(len(moni_gaps)):
        t_start = moni_gaps[i][0]
        new_bins = go.io.get_telemetry_binaries((t_start - 300), t_start, data_dir=args.telemetry_dir)

        evt_hb_list = []
        mtb_hb_list = []


        for binary in new_bins:
            treader = go.io.TelemetryPacketReader(str(binary))
            for p in treader:
                gcu0 = p.header.gcutime
                if int(p.header.packet_type) == 92: #AnyTofHKP
                    tp_hb = go.io.TofPacket()
                    tp_hb.from_bytestream(p.payload, 0)
                  
                    if int(tp_hb.packet_type) == 62: #MTBHeartbeat
                        mtb_hb = go.tof.monitoring.MTBHeartbeat()
                        mtb_hb.from_tofpacket(tp_hb)
                        mtb_hb_list.append((gcu0,mtb_hb))
                    
                    if int(tp_hb.packet_type) == 63: #EVTBLDRHeartbeat
                        evt_hb = go.tof.monitoring.EVTBLDRHeartbeat()
                        evt_hb.from_tofpacket(tp_hb)
                        evt_hb_list.append((gcu0, evt_hb))


        evt_hb_list.sort()
        mtb_hb_list.sort()

        j=0
        while (j < len(mtb_hb_list)) and (mtb_hb_list[j][0] < t_start):
            j += 1

        if mtb_hb_list:
            mtb_hb = mtb_hb_list[j-1][1]
            mtb_data = (
                mtb_hb.total_elapsed,
                mtb_hb.n_events,
                mtb_hb.evq_num_events_last,
                mtb_hb.n_ev_unsent,
                mtb_hb.n_ev_missed
                )
        else:
            mtb_data = (None, None, None, None, None)


        i=0
        while (i < len(evt_hb_list)) and (evt_hb_list[i][0] < t_start):
            i += 1


        if evt_hb_list:
            evt_hb = evt_hb_list[i-1][1]
            evt_data = (
                evt_hb.n_mte_received_tot,
                evt_hb.n_rbe_received_tot,
                evt_hb.n_mte_skipped,
                evt_hb.n_timed_out,
                evt_hb.event_cache_size,
                evt_hb.event_cache_size
            )
        else:
            evt_data = (None, None, None, None, None, None)

        moni_gaps[i] = (*moni_gaps[i], mtb_data, evt_data)


    evt_hb_list.clear()
    gc.collect()
    mtb_hb_list.clear()
    gc.collect()
    
    output_file = f'/home/gtytus/mtb-moni-log/reports/12Dec/{args.start_time}_{args.end_time}_mtb_outages_report.txt'  

    with open(output_file, 'w') as f:
        f.write(f'the start time was {dt_start.strftime('%Y-%m-%d %H:%M:%S UTC')} and the first MTBMoniData was received {dt1.strftime('%Y-%m-%d %H:%M:%S UTC')}\n')
        f.write(f'the end time was {dt_end.strftime('%Y-%m-%d %H:%M:%S UTC')} and the last MTBMoniData was received {dt2.strftime('%Y-%m-%d %H:%M:%S UTC')}')
        f.write('--------------------------------------------------------------------------------------------\n')
        f.write(f'Detected {len(moni_gaps)} MTB outages with length greater than {args.window} seconds between {args.start_time} to {args.end_time} seconds\n')
        f.write('')
        for gap in moni_gaps:
            f.write(f'from {gap[0]} to {gap[1]} with duration {gap[2]}\n')
            f.write(f'---the tiu_busy_count before the crash was {gap[3]}\n')
            f.write(f'---the daq queue length before the crash was {gap[4]}\n')
            f.write(f'---the temperature before the crash was {gap[5]}\n')
            f.write(f'---the rate before the crash was {gap[6]}\n')
            f.write(f'---the lost rate before the crash was {gap[7]}\n')
            f.write('')
            f.write(f'---the elapsed time before the crash was {gap[11][0]}\n')
            f.write(f'---the num. events received before the crash was {gap[11][1]}\n')
            f.write(f'---the event queue size before the crash was {gap[11][2]}\n')
            f.write(f'---the num. unsent events before the crash was {gap[11][3]}\n')
            f.write(f'---the num. missed events before the crash was {gap[11][4]}\n')
            f.write('')
            f.write(f'---the MTEvent receiver len. before the crash was {gap[12][0]}\n')
            f.write(f'---the RBEvent receiver len. before the crash was {gap[12][1]}\n')
            f.write(f'---the num. MTEvents skipped before the crash was {gap[12][2]}\n')
            f.write(f'---the num. timed out events before the crash was {gap[12][3]}\n')
            f.write(f'---the cache size before the crash was {gap[12][4]}\n')
            f.write(f'---the event ID cache size before the crash was {gap[12][5]}\n')
            f.write('')
            f.write(f'---the vcc aux before the crash was {gap[8]}\n')
            f.write(f'---the vcc aux before the crash was {gap[9]}\n')
            f.write(f'---the vcc aux before the crash was {gap[10]}\n')
            f.write('-------------------------------------------------------------------------------------------\n')

        f.write('--------------------------------------------------------------------------------------------\n')

    print('Detected ' + str(len(moni_gaps))+ ' MTB outages with lenth greater than '+ str(args.window) + ' seconds between ' + str(args.start_time) + ' to '+ str(args.end_time))
    print(f"Additional output written to {output_file}")

    plt.hist(hdurations, bins=100, histtype = 'step')
    plt.yscale('log')
    plt.title(f'\u0394t between MTBMoniData')
    plt.ylabel('n')
    plt.xlabel('seconds')
    plt.minorticks_on()
    plt.savefig(f'/home/gtytus/mtb-moni-log/reports/12Dec/{args.start_time}_to_{args.end_time}_MTBMoniData_Interval.pdf')

