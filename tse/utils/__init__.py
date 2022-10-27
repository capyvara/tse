__version__ = '0.1.0'

import logging

# TODO: Check tqdm: https://github.com/tqdm/tqdm/issues/313
def log_progress(iterable, total=None, format=None, batch_size=None):
    if not total:
        if hasattr(iterable, '__len__'):
            total = len(iterable)

    if total:
        batch_size = batch_size or total / 10
        format = "Processed %d / %d (%.0f %%)"
    else:
        batch_size = batch_size or 1
        format = "Processed %d"

    batch_size = int(batch_size)

    count = 0
    for item in iterable:
        yield item
        
        count += 1
        if not total:
             if count == 1 or count % batch_size == 0:
                logging.info(format, count)
        else:
            if count == 1 or count % batch_size == 0 or count == total:
                # Delay logging last batch to last iteration
                if count != total and count == (total - (total % batch_size)):
                    continue

                percent = (count / total) * 100.0
                logging.info(format, count, total, percent)
