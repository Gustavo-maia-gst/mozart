import type { ProtocolSpi } from '@mozart/contracts';
import type { Type } from '@nestjs/common';
import { EchoProtocol } from './echo';

/** Name -> protocol class. Slaves resolve their protocol by MOZART_PROTOCOL. */
export const PROTOCOLS: Record<string, Type<ProtocolSpi>> = {
  echo: EchoProtocol,
};

export function resolveProtocol(name: string): Type<ProtocolSpi> {
  const protocol = PROTOCOLS[name];
  if (!protocol) {
    throw new Error(`unknown protocol "${name}"; known: ${Object.keys(PROTOCOLS).join(', ')}`);
  }
  return protocol;
}
