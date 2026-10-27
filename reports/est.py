import sys, zipfile, re, math
from xml.etree import ElementTree as ET
NS={'w':'http://schemas.openxmlformats.org/wordprocessingml/2006/main','wp':'http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing'}
W='{%s}'%NS['w']
CHAR=0.49
d=ET.fromstring(zipfile.ZipFile(sys.argv[1]).read('word/document.xml'))
body=d.find('w:body',NS)
pg=body.find('w:sectPr/w:pgSz',NS); mar=body.find('w:sectPr/w:pgMar',NS)
PAGE_H=(int(pg.get(W+'h'))-int(mar.get(W+'top'))-int(mar.get(W+'bottom')))/20
CONTENT_W=(int(pg.get(W+'w'))-int(mar.get(W+'left'))-int(mar.get(W+'right')))/20
def para_h(p,width):
    ext=p.find('.//wp:extent',NS)
    sp=p.find('w:pPr/w:spacing',NS); after=before=0; line=240
    if sp is not None: after=int(sp.get(W+'after',0))/20; before=int(sp.get(W+'before',0))/20; line=int(sp.get(W+'line',240))
    if ext is not None: return before+after+int(ext.get('cy'))/12700
    txt=''.join(t.text or '' for t in p.iter(W+'t'))
    sz=None
    for s in p.iter(W+'sz'): sz=int(s.get(W+'val'))/2; break
    sz=sz or 10
    w=width
    if p.find('w:pPr/w:numPr',NS) is not None: w-=15
    cpl=max(1,w/(sz*CHAR)); lines=max(1,math.ceil(len(txt)/cpl)) if txt else 1
    return before+after+lines*sz*1.22*line/240
def block_h(el,width):
    if el.tag==W+'p': return para_h(el,width)
    if el.tag==W+'tbl':
        ws=[int(g.get(W+'w'))/20 for g in el.findall('w:tblGrid/w:gridCol',NS)]; tot=0
        for tr in el.findall('w:tr',NS):
            tot+=max(sum(para_h(p,ws[i]-7) for p in tc.findall('w:p',NS))+3 for i,tc in enumerate(tr.findall('w:tc',NS)))
        return tot
    return 0
tot=sum(block_h(e,CONTENT_W) for e in body)
print(sys.argv[1].split('/')[-1],'est pages: %.2f'%(tot/PAGE_H))
