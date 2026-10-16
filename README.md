# Pokeidle Manager

> **Native Rust migration (experimental):** A Tauri/WebView2-free egui target is now available in [native/](native/README.md). It reuses Moth Firefox profiles and the existing Manager game engine. It is not feature-complete; the original desktop app remains available.

O Pokeidle Manager é um aplicativo complementar não oficial para desktop, feito para organizar várias contas do Pokeidle e consultar informações de conta, hunts, inventário, automações e mercado. O projeto não é afiliado, endossado nem operado pelo jogo Pokeidle ou pelos detentores de seus direitos.

Este repositório é o código-fonte oficial da edição comunitária do Pokeidle Manager.

## Download

A versão estável **v0.1.0 ainda não foi publicada**. A versão candidata pública atual e recomendada para testes é **v0.1.0-rc.4 (RC4)**. A RC3, RC2 e RC1 continuam disponíveis apenas para histórico e reprodutibilidade.

Baixe o aplicativo exclusivamente pela [página oficial da RC4](https://github.com/DiuleJR/pokeidle-manager/releases/tag/v0.1.0-rc.4) ou pela página de [GitHub Releases](https://github.com/DiuleJR/pokeidle-manager/releases). Para a maioria dos usuários, recomendamos o instalador NSIS:

`Pokeidle.Manager_0.1.0_x64-setup.exe`

O MSI e o arquivo `SHA256SUMS.txt` para verificação de integridade também estão disponíveis na página da RC4. Esta é uma pré-release para testes e pode conter bugs; não a confunda com uma versão estável.

## Compilar localmente

O aplicativo para desktop usa Tauri 2, Rust, Node.js e npm. No Windows, instale os pré-requisitos descritos no [guia oficial de pré-requisitos do Tauri v2](https://v2.tauri.app/start/prerequisites/) e execute:

```powershell
npm ci
npm run check
npm run build
npm run tauri:build:community
```

A edição comunitária é compilada sem um serviço comercial de licenciamento. O servidor opcional do painel mobile serve apenas para desenvolvimento; não o habilite em uma versão empacotada.

## Instalação no Windows

O aplicativo é destinado ao Windows 10 e Windows 11. Use somente os instaladores publicados na página oficial da [RC4](https://github.com/DiuleJR/pokeidle-manager/releases/tag/v0.1.0-rc.4); não use instaladores enviados por canais ou links não verificados. As versões [RC3](https://github.com/DiuleJR/pokeidle-manager/releases/tag/v0.1.0-rc.3), [RC2](https://github.com/DiuleJR/pokeidle-manager/releases/tag/v0.1.0-rc.2) e [RC1](https://github.com/DiuleJR/pokeidle-manager/releases/tag/v0.1.0-rc.1) permanecem disponíveis para histórico e reprodutibilidade.

O instalador não inclui o Brave. Para entrar ou reabrir uma conta no modo Browser, instale o Brave pelo [site oficial](https://brave.com/pt-br/download/); o Manager não baixa nem distribui o navegador.

Os dados do Manager e os perfis do Brave ficam no computador. Perfis podem conter cookies e sessões autenticadas. Nunca envie cookies, tokens, perfis do navegador, bancos SQLite reais, credenciais ou logs brutos em Issues; consulte [Privacidade](PRIVACY.md) e [Política de segurança](SECURITY.md) antes de relatar um problema.

Os instaladores da RC4 ainda não possuem assinatura digital. O Windows SmartScreen pode exibir um aviso; confira a origem do arquivo e baixe-o somente pela página oficial de Releases.

## Requisito de navegador

O Pokeidle Manager utiliza o Brave para abrir e gerenciar contas no modo Browser e durante o login. O Brave não é distribuído junto com o Manager nem é baixado automaticamente pelo aplicativo. Se não estiver instalado, o Manager orientará você a obtê-lo no site oficial do Brave. Depois que uma sessão autenticada é transferida para o modo Background, a conexão é gerenciada pelo próprio Manager; abrir o Browser ou refazer login continua exigindo o Brave.

## Dados e rede

O aplicativo armazena o banco de dados SQLite, os recursos em cache e os perfis persistentes do navegador Brave de cada conta no diretório de dados do aplicativo definido pelo Tauri no sistema operacional. Os perfis do navegador podem conter cookies de login e outros dados de sessão. Remover uma conta do aplicativo não necessariamente remove seu perfil do navegador. Consulte [Privacidade](PRIVACY.md) para saber mais e ver os cuidados antes de apagar dados.

O aplicativo se conecta ao site e aos serviços do Pokeidle para obter dados do jogo e manter sessões, e pode buscar recursos durante a execução. As recomendações de hunt usam um script de referência comunitário obtido de `guiapokeidlehardtocapture.site`; o repositório aponta para a fonte, mas não redistribui esse script. Sprites do jogo e artes de terceiros sem autorização de redistribuição não são incluídos. Consulte [Ativos e ícones](docs/ASSETS.md).

## Comunidade e suporte

- Discord oficial: [discord.gg/Hup5P6jD7](https://discord.gg/Hup5P6jD7)
- GitHub oficial: [DiuleJR/pokeidle-manager](https://github.com/DiuleJR/pokeidle-manager)

Use esses canais para dúvidas, feedback, testes, sugestões e relatos de bugs. **Não publique cookies, tokens, credenciais, perfis do Brave, bancos SQLite reais, sessões autenticadas ou logs brutos com dados sensíveis.** Consulte [Privacidade](PRIVACY.md) e [Política de segurança](SECURITY.md) antes de relatar um problema. Para contribuir, leia também [Como contribuir](CONTRIBUTING.md), o [Código de Conduta](CODE_OF_CONDUCT.md) e [Marcas e afiliação](TRADEMARKS.md).

## Licença

O código do projeto é disponibilizado sob a licença **GPL-3.0-only**; consulte [LICENSE](LICENSE). Componentes e dados de terceiros podem ter termos próprios. A revisão das licenças das dependências ainda não foi concluída; consulte [Avisos de terceiros](THIRD_PARTY_NOTICES.md). Este texto não constitui uma conclusão jurídica.
