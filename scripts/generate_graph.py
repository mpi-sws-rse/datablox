#!/usr/bin/env python
# -*- py-indent-offset:2 -*-

import sys
from os.path import abspath, join, dirname, exists, expanduser

from optparse import OptionParser

framework_path = abspath(expanduser(join(dirname(__file__), "../datablox_framework/datablox_framework")))
if not exists(framework_path):
  raise Exception("Datablox framework package %s does not exist" % framework_path)

sys.path.append(framework_path)
import topology

def mangle(n):
    return n.replace("-", "_")

def gen_graph(t, f, edge_labels=True):
  def write_block(b, indent_level):
    f.write('%s%s [shape=box, label="%s"];\n' %
            (' '*indent_level, mangle(b.block_id), b.block_id))      
  f.write("digraph G {\n")
  for n in t.nodes.keys():
    f.write("  subgraph cluster_%s {\n" % mangle(n))
    f.write('    label = "%s";\n' % n)
    f.write('    shape = rectangle;\n')
    f.write('    style=filled;\n')
    f.write('    color=lightgrey;\n')
    blocks_in_node = t.nodes[n]
    for b in blocks_in_node:
        write_block(b, 4)
    f.write("  }\n")
  for b in t.unmapped_blocks:
      write_block(b, 2)
  for (sb, sp, db, dp) in t.connections:
      label = "%s => %s" % (sp, dp) \
              if (not db.is_shard_element) and edge_labels \
              else ""
      f.write('  %s -> %s [label="%s"];\n' %
              (mangle(sb.block_id), mangle(db.block_id), label))
  f.write("}\n") # end of G
  
def main(argv):
  usage = "%prog [options] topology_file"
  parser = OptionParser(usage=usage)
  parser.add_option("--no-edge-labels",
                    dest="no_edge_labels",
                    default=False,
                    action="store_true",
                    help="If specified, do not include labels on edges")
  (options,args) = parser.parse_args(argv)
  if len(args)==0:
    parser.print_help()
    return 1
  topology_file = args[0]
  t = topology.Topology.create(topology_file)
  t.expand_shards()
  gen_graph(t, sys.stdout, edge_labels=(not options.no_edge_labels))
  return 0

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))


