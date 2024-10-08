import { FastenerTemplate } from '@swim/component';
import { MapDownlink, WarpClient, WarpRef } from '@swim/client';
import { useEffect, useRef } from 'react';

export const useMapDownlink = <K, V>(options: FastenerTemplate<MapDownlink<WarpRef, K, V>>): MapDownlink<WarpRef, K, V> => {
  const { hostUri, nodeUri, laneUri, keyForm, valueForm, didUpdate, didRemove } = options;

  const client = useRef<WarpClient>(new WarpClient());
  const mapDownlinkRef = useRef<MapDownlink<WarpRef, K, V>>(
    client.current.downlinkMap<K, V>({
      hostUri,
      nodeUri,
      laneUri,
      keyForm,
      valueForm, // coerces content of WARP message to strongly-typed JS object
      didUpdate,
      didRemove,
    })
  );

  useEffect(() => {
    mapDownlinkRef.current.didUpdate = didUpdate;
  }, [didUpdate]);

  useEffect(() => {
    mapDownlinkRef.current.didRemove = didRemove;
  }, [didRemove]);

  return mapDownlinkRef.current;
}
