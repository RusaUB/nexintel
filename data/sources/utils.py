import os, time, json, pickle, hashlib
from functools import wraps

def ttl_cache(ttl_seconds, cache_dir=".cache", key_fn=None):
    """
    Файловый кэш (pickle) с TTL.
    - Имя файла стабильно: md5(JSON-ключа).
    - В pickle лежит {"result": ..., "timestamp": ...}.
    - key_fn(self, *args, **kwargs) -> dict  позволяет задать собственный ключ.
    """
    os.makedirs(cache_dir, exist_ok=True)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            inst = args[0] if args else None  
            base_key = {"func": f"{func.__module__}.{func.__qualname__}"}

            ns = getattr(inst, "cache_ns", None)
            if ns is not None:
                base_key["ns"] = ns

            if key_fn is not None:
                custom = key_fn(inst, *args[1:], **kwargs)
                if isinstance(custom, dict):
                    base_key["custom"] = custom
                else:
                    base_key["custom"] = str(custom)
            else:
                base_key["kwargs"] = kwargs

            key_str = json.dumps(base_key, sort_keys=True, default=str)
            fname = hashlib.md5(key_str.encode("utf-8")).hexdigest() + ".pkl"
            fpath = os.path.join(cache_dir, fname)

            now = time.time()

            if os.path.exists(fpath):
                try:
                    with open(fpath, "rb") as f:
                        data = pickle.load(f)
                    if isinstance(data, dict) and "timestamp" in data:
                        age = now - data["timestamp"]
                        if age < ttl_seconds:
                            print(f"[CACHE] Using cached result for {base_key['func']} (age={age/3600:.2f}h)")
                            return data["result"]
                        else:
                            print(f"[CACHE] Cache expired for {base_key['func']} (age={age/3600:.2f}h), refreshing...")
                    else:
                        print(f"[CACHE] Invalid cache format, refreshing...")
                except Exception as e:
                    print(f"[CACHE] Read error ({e}), refreshing...")

            result = func(*args, **kwargs)
            try:
                with open(fpath, "wb") as f:
                    pickle.dump({"result": result, "timestamp": now}, f)
                print(f"[CACHE] Saved new cache for {base_key['func']}")
            except Exception as e:
                print(f"[CACHE] Write error ({e}), skip caching.")

            return result
        return wrapper
    return decorator


def build_mentions_text(rows, category="crypto"):
    positive_changes = []
    negative_changes = []

    for i in rows:
        if i["mention_growth"] > 0:
            positive_changes.append((i["symbol"], i["mention_growth"]))
        else:
            negative_changes.append((i["symbol"], i["mention_growth"]))

    text = ""

    if positive_changes:
        text += f"Over the past week, {category} mentions showed strong growth:\n"
        max_idx = min(5, len(positive_changes))
        info = ", ".join(
            [f"{positive_changes[j][0]} (+{round(positive_changes[j][1]*100)}%)"
             for j in range(max_idx)]
        )
        text += info + ".\n"

    if negative_changes:
        max_idx = min(5, len(negative_changes))
        info = ", ".join(
            [f"{negative_changes[j][0]} ({round(negative_changes[j][1]*100)}%)"
             for j in range(max_idx)]
        )
        text += info + " recorded declines, signaling reduced community interest.\n"

    return text