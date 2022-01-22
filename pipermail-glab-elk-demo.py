from datetime import datetime

from grimoire_elk.raw.pipermail import PipermailOcean
from grimoire_elk.enriched.pipermail import PipermailEnrich

from grimoire_elk.utils import get_elastic
from perceval.backends.core.pipermail import Pipermail
import os

pipermail_backend = Pipermail('https://sourceware.org/pipermail/libc-announce', 'libc-announce')
pipermail_ocean = PipermailOcean(None) # The backend has to be None, or   the inserted data can't be enriched

OS_HOST = os.environ.get('OS_HOST') or '192.168.8.10'
OS_PORT = os.environ.get('OS_PORT') or 9200
OS_USER = os.environ.get('OS_USER') or 'admin'
OS_PASS = os.environ.get('OS_PASS') or 'admin'

OS_URL = f'https://{OS_USER}:{OS_PASS}@{OS_HOST}:{OS_PORT}'

elastic = get_elastic(OS_URL, 'test_pipermail_raw', True, pipermail_ocean, [])
pipermail_ocean.set_elastic(elastic)


def data2es(items, ocean):
    def ocean_item(item):
        # Hack until we decide when to drop this field
        if 'updated_on' in item:
            updated = datetime.fromtimestamp(item['updated_on'])
            item['metadata__updated_on'] = updated.isoformat()
        if 'timestamp' in item:
            ts = datetime.fromtimestamp(item['timestamp'])
            item['metadata__timestamp'] = ts.isoformat()

        # the _fix_item does not apply to the test data for Twitter
        try:
            ocean._fix_item(item)
        except KeyError:
            pass

        if ocean.anonymize:
            ocean.identities.anonymize_item(item)

        return item

    items_pack = []  # to feed item in packs

    for item in items:
        item = ocean_item(item)
        if len(items_pack) >= ocean.elastic.max_items_bulk:
            ocean._items_to_es(items_pack)
            items_pack = []
        items_pack.append(item)
    inserted = ocean._items_to_es(items_pack)

    return inserted

# Store original data to raw opensearch index
data2es(pipermail_backend.fetch(), pipermail_ocean)


# TODO What does the param [sortinghat] and [projects] do here?
enrich_backend = PipermailEnrich()
elastic_enrich = get_elastic(OS_URL, 'test_pipermail_enrich', True, enrich_backend, [])
enrich_backend.set_elastic(elastic_enrich)

num_enriched = enrich_backend.enrich_items(pipermail_ocean)
print(f'num_enriched: {num_enriched}')