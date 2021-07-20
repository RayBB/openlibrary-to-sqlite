# everything about getting the dump
import urllib.request
from tqdm import tqdm

## Taken right from the docs at https://github.com/tqdm/tqdm
class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        return self.update(b * bsize - self.n)  # also sets self.n = b * bsize

def download_file_with_progress_bar(url, destination):
    with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1,
                desc=url.split('/')[-1]) as t:  # all optional kwargs
        urllib.request.urlretrieve(url, filename=destination,
                        reporthook=t.update_to, data=None)
        t.total = t.n
