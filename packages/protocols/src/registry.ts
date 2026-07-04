import type { Type } from '@nestjs/common';
import { BaselineProtocol } from './baseline';
import type { Protocol } from './protocol';

/** Name -> protocol class. Slaves resolve their protocol by MOZART_PROTOCOL. */
export const PROTOCOLS: Record<string, Type<Protocol>> = {
  baseline: BaselineProtocol,
};

export function resolveProtocol(name: string): Type<Protocol> {
  const protocol = PROTOCOLS[name];
  if (!protocol) {
    throw new Error(`unknown protocol "${name}"; known: ${Object.keys(PROTOCOLS).join(', ')}`);
  }
  return protocol;
}
