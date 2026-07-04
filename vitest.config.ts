import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

// SWC (instead of esbuild) so `emitDecoratorMetadata` works — NestJS DI relies on
// design:paramtypes metadata that esbuild cannot emit.
export default defineConfig({
  plugins: [
    swc.vite({
      module: { type: 'es6' },
      jsc: {
        parser: { syntax: 'typescript', decorators: true },
        transform: { legacyDecorator: true, decoratorMetadata: true },
        target: 'es2022',
      },
    }),
  ],
  test: {
    include: [
      'apps/*/src/**/*.spec.ts',
      'apps/*/test/**/*.spec.ts',
      'packages/*/src/**/*.spec.ts',
      'packages/*/test/**/*.spec.ts',
    ],
    environment: 'node',
    testTimeout: 15_000,
    hookTimeout: 30_000,
  },
});
