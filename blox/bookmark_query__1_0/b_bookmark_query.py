from block import *
from BeautifulSoup import BeautifulSoup 
from logging import ERROR, WARN, INFO, DEBUG
import urlparse

class bookmark_query(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("list", Port.QUERY, Port.UNNAMED, [])
    self.add_port("restore", Port.QUERY, Port.UNNAMED, ["url", "time"])
    self.add_port("delete", Port.QUERY, Port.UNNAMED, ["url", "time"])
    self.add_port("store_control", Port.QUERY, Port.UNNAMED, ["command", "args"])
    self.add_port("meta_control", Port.QUERY, Port.UNNAMED, ["command", "args"])
  
  def add_meta(self, log):
    mlog = Log()
    mlog.append_field("path", log.log["internet_url"])
    #we only have one fingerprint per url now, so create a one element list
    mlog.append_field("fingerprints", [[f] for f in log.log["fingerprint"]])
    self.push("meta_store", mlog)

  def add_chunks(self, log):
    clog = Log()
    clog.append_field("url", log.log["url"])
    clog.append_field("fingerprint", log.log["fingerprint"])
    self.push("store", clog)

  def recv_push(self, port, log):
    self.add_chunks(log)
    self.add_meta(log)
  
  def fetch_meta(self, url, time):
    mlog = Log()
    mlog.append_field("command", ["restore"])
    mlog.append_field("args", [[(url, time)]])
    retlog = self.query("meta_control", mlog)
    asset_list = retlog.log["assets"]
    self.log(INFO, "got assets from meta_store: %r" % asset_list)
    assert(len(asset_list)==1)
    return asset_list[0]
  
  def fetch_store(self, fp):
    slog = Log()
    slog.append_field("command", ["restore"])
    slog.append_field("args", [[fp]])
    retlog = self.query("store_control", slog)
    store_urls = retlog.log["chunk"]
    self.log(INFO, "got urls from data_store: %r" % store_urls)
    assert(len(store_urls)<=1)
    if len(store_urls) == 0:
      raise KeyError
    return store_urls[0]
    
  def rewrite_links(self, url, html, assets):
    soup = BeautifulSoup(html)
    img_links = [l.get('src') for l in soup.findAll('img')]
    css_links = [l.get('href') for l in soup.findAll('link') if l.has_key('rel') and l['rel'].lower() == 'stylesheet']
    #extract links with valid sources
    links = [l for l in (img_links + css_links) if l != None]
    local_links = {}
    for l in links:
      #convert relative links into absolute
      try:
        fp = assets[urlparse.urljoin(url, l) if l[:4] != 'http' else l]
        local_links[l] = self.fetch_store(fp)
      except KeyError:
        self.log(WARN, "did not download url for link: %r" % l)
        #we did not dowload that url
        local_links[l] = l
      
    img_tags = soup.findAll('img')
    for i in img_tags:
      i['src'] = local_links[i['src']]
    css_tags = [t for t in soup.findAll('link') if t.has_key('rel') and t['rel'].lower() == 'stylesheet']
    for c in css_tags:
      c['href'] = local_links[c['href']]

    return soup.prettify()
    
  def restore(self, url, time):
    try:
      asset_pairs = self.fetch_meta(url, time)
      assets = {}
      for aurl, fp in asset_pairs:
        assets[aurl] = fp
      local_url = assets[url]
      html = BlockUtils.fetch_file_at_url(self.fetch_store(local_url),
                                          self.ip_address)
      html = self.rewrite_links(url, html, assets)
      name = unicode('bookmark_restored')
      with open(name, 'w') as f:
        f.write(html)
      path = os.path.join(os.getcwd(), name)
      return BlockUtils.generate_url_for_path(path, self.ip_address)
    except KeyError:
      self.log(WARN, "could not restore file: %r" % url)
      return ''
  
  def delete(self, url, time):
    try:
      asset_pairs = self.fetch_meta(url, time)
      fps = [a[1] for a in asset_pairs]
      mlog = Log()
      mlog.append_field("command", ["delete"])
      mlog.append_field("args", [[(url, time)]])
      res = self.query("meta_control", mlog).log["result"][0]
      slog = Log()
      slog.append_field("command", ["delete"])
      slog.append_field("args", [fps])
      deleteres = self.query("store_control", slog).log["result"]
      for r in deleteres:
        res = res and r
      return res
    except Exception as e:
      self.log(WARN, "could not delete %r (at %r) due to %r" % (url, time, e))
      return False      
    
  def list_bookmarks(self):
    mlog = Log()
    mlog.append_field("command", ["list"])
    mlog.append_field("args", [[]])
    retlog = self.query("meta_control", mlog)
    return (retlog.log["path"], retlog.log["time"])
    
  def recv_query(self, port, log):
    retlog = Log()
    if port == "list":
      urls, times = self.list_bookmarks()
      retlog.append_field("url", urls)
      retlog.append_field("time", times)
    elif port == "delete":
      delete_res = [self.delete(u, t) for u, t in log.iter_fields("url", "time")]
      retlog.append_field("result", delete_res)
    elif port == "restore":
      restored_urls = [self.restore(u, t) for u, t in log.iter_fields("url", "time")]
      retlog.append_field("url", restored_urls)
    
    self.return_query_res(port, retlog)
