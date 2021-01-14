(window as any).__webpack_public_path__ =
  document.querySelector('body')!.getAttribute('data-base-url') +
  'nbextensions/jupyter-graphin';

export * from './index';
