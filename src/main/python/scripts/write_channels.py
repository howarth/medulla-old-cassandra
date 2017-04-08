from __future__ import print_function
import mne
import numpy as np
import argparse
import os, os.path

def assert_correct_mne():
  return True

def ensure_dir_exists(directory):
  if not os.path.exists(directory):
    os.makedirs(directory)

_channel_name_pattern = "CHANNEL-NAME"
_time_name = "time"

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--files', required=True)
  parser.add_argument('--channel-type', required=True, choices=['stim', 'all'])

  args = parser.parse_args()

  with open(args.files, 'r') as f:
    files = [tuple(l.strip().split(' ')) for l in f.readlines()]

  for (input, out) in files:
    if _channel_name_pattern not in out:
      raise ValueError("'%s' must exist in each output file path, which is used as a place holder for the channel's name" % _channel_name_pattern)

    r = mne.io.Raw(input, preload=True)

    if args.channel_type == 'stim':
      r = r.pick_types(stim=True, meg=False)
    else:
      pass

    data = r._data
    channel_names = r.info['ch_names']
    for i, ch_name in enumerate(channel_names):
      out_fname = out.replace(_channel_name_pattern, ch_name)
      ensure_dir_exists(os.path.dirname(out_fname))
      with open(out_fname, "wb") as fid:
        np.array([1], dtype=">f8").tofile(fid)
        np.array( data[i,:], dtype=">f8").tofile(fid)


    out_fname = out.replace(_channel_name_pattern, "channels")
    with open(out_fname, "w") as fid:
      for ch in channel_names:
        print("%s" % ch, file=fid)

    out_fname = out.replace(_channel_name_pattern, "time")
    with open(out_fname, "w") as fid:
      print("%.4f" % r.times[0], file=fid)
      print("%.3f" % r.times[-1], file=fid)
      for t in r.times:
        print("%.3f" % t, file=fid)


    
