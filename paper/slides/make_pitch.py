#!/usr/bin/env python3
"""Elevator-pitch deck: nine slides of the full deck, with the narration script as speaker notes.

Usage: python paper/slides/make_pitch.py <full deck.pptx> <pitch deck.pptx>
"""

import sys

from pptx import Presentation

# Slide number in the full deck -> narration (about 2 minutes 45 seconds in total)
SCRIPT = {
    1: "This is SmokeStream, by Vraj Patel and Brandon Moriguchi. The question: how fast can a smoke alert be?",
    3: "On September 9, 2020, the sky over the Bay Area turned orange. Most people checked crowdsourced "
       "PurpleAir maps, not the twenty official monitors. Those maps are real-time systems: readings arrive late, "
       "sensors drop out, and satellite fire data is hours old. The methods behind them were validated in hindsight. "
       "Nobody had measured what deciding in real time actually costs.",
    6: "So we built the system twice. A replay tool feeds real fire-season data into Kafka, with arrival delays we "
       "measured from two hundred live sensors. Spark Structured Streaming corrects each reading, maps it to an H3 "
       "hexagon, waits for late data with a watermark, checks it against its neighbours, and labels smoke using fire, "
       "plume and wind data that Airflow keeps fresh. Results go live to MongoDB Atlas. A batch job runs the same code "
       "with perfect hindsight, and Snowflake holds both side by side.",
    10: "First finding: waiting for late data is almost free. The stream stays within 0.12 micrograms of perfect "
        "hindsight, while the sensors themselves are off by 17. A longer watermark just delays the alert.",
    11: "Second: what really costs accuracy is stale context. Refreshing fire and plume data every six hours instead "
        "of every fifteen minutes drops label agreement from 92 to 84 percent, and to 55 percent around one July Fourth.",
    13: "Third: one laptop keeps up. Kafka and Spark handle 640 messages a second, and real readings reach the live "
        "map in about 16 seconds, most of it designed waiting. The smoke alerts were 88 percent precise and arrived a "
        "median 99 minutes before the official hourly reading.",
    16: "We also used course techniques where the problem needs them: a Bloom filter that drops Kafka's duplicate "
        "deliveries, DGIM and Flajolet-Martin sketches that count in a few bytes, LSH to find sensors that move "
        "together, differential privacy for sensors at people's homes, and a poisoning test that shows where fake "
        "sensors can fool the quality check.",
    24: "The takeaway for anyone building real-time alerts: don't over-wait for data, keep your context fresh.",
    25: "The code, the pipeline and the paper are all on GitHub. Thanks for watching.",
}


def main(src, dst):
    prs = Presentation(src)
    ids = prs.slides._sldIdLst
    for number, sld in reversed(list(enumerate(list(ids), start=1))):
        if number not in SCRIPT:
            prs.part.drop_rel(sld.rId)  # the slide part is then left out of the saved file
            ids.remove(sld)
    for number, slide in zip(sorted(SCRIPT), prs.slides):
        slide.notes_slide.notes_text_frame.text = SCRIPT[number]
        for shape in slide.shapes:
            if shape.has_text_frame and shape.text_frame.text.startswith("Thank you. Questions?"):
                shape.text_frame.paragraphs[0].runs[0].text = "Thanks for watching"
    prs.save(dst)
    print(f"Wrote {dst}: {len(prs.slides)} slides, about {sum(len(s.split()) for s in SCRIPT.values())} words of narration")


if __name__ == "__main__":
    main(*sys.argv[1:3])
