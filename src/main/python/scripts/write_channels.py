import mne
import argparse

def assert_correct_mne():
  return True

_channel_name_pattern = "CHANNEL_NAME"
_time_name = "time"

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--input-files', required=True)
  parser.add_argument('--output-files', required=True)
  parser.add_argument('--channel-type', 'required=True, choices=['stim'])

  args = parser.parse_args()

  with open(args.input_files, 'r') as f:
    inputs = f.readlines()

  with open(args.output_files, 'r') as f:
    outputs = f.readlines()

  if len(inputs) != len(outputs):
    raise ValueError("The input and output files should have the same number of lines")

  for in, out in zip(inputs, outputs):
    if _channel_name_pattern not in out:
      raise ValueError("'%s' must exist in each output file path, which is used as a place holder for the channel's name" % _channel_name_pattern)

    r = mne.io.Raw(in, preload=True)

    if args.channel_type == 'stim':
      r = r.pick_types(r.info, stim=True, meg=False)
    else:
      raise ValueError("Unimplemented channel type")

    data = r._data
    channel_names = r.info['ch_names']
    for i, ch_name in enumerate(channel_names):
      out_fname = out.replace(_channel_name_pattern, ch_name)
      with open(out_fname, "wb") as fid:
        np.array([1] + data[i,:], dtype=">f8").tofile(fid)
