# concurrent-counter

[日本語](README.md) | [English](README.en.md)

ミークの課題で使う、数を集計するプログラムです。Go の標準ライブラリだけを使っています。

## 必要なもの

- Bash、PowerShell、コマンドプロンプトなどのターミナル
- ソースコードから実行する場合は Go 1.22 以上: <https://go.dev/dl/>
- Git（リポジトリを clone する場合）

ソースコードから実行する場合は、Go が使えるか確認します。

```text
go version
```

Go 1.22 以上が表示されたら、準備は完了です。このプロジェクトは Go の標準ライブラリだけを使うので、ほかのパッケージやサービスは必要ありません。

## 実行する方法

### 目次

1. [方法 1: ソースコードから実行する](#run-from-source)
2. [方法 2: Release をダウンロードする](#download-release)
3. [方法 3: Docker で実行する](#run-with-docker)

<a id="run-from-source"></a>

### 方法 1: ソースコードから実行する

プログラムを実行するパソコンで、次の手順を行ってください。

#### ステップ 1: Go をインストールする

<https://go.dev/dl/> から Go 1.22 以上をインストールします。インストールした後、ターミナルを一度閉じて、もう一度開いてください。

次のコマンドを実行します。

```text
go version
```

次の例のように Go 1.22 以上が表示されたら、次へ進みます。

```text
go version go1.22.0 windows/amd64
```

<a id="download-source"></a>

#### ステップ 2: ソースコードをダウンロードする

ブラウザでダウンロードする方法と、Git で clone する方法があります。どちらを選んでも同じファイルを取得できます。

##### 方法 A: Git を使わずに GitHub からダウンロードする

1. ブラウザで <https://github.com/IshmamAbir/process-thread-aggregator> を開きます。
2. ファイル一覧の上にある緑色の **Code** ボタンをクリックします。
3. メニューの **Download ZIP** をクリックします。
4. `process-thread-aggregator-main.zip` のような名前の ZIP ファイルがダウンロードされるまで待ちます。
5. ZIP ファイルを展開します。

   - Windows では、ZIP ファイルを右クリックします。**すべて展開**を選び、保存する場所を決めて、**展開**をクリックします。
   - macOS では、ZIP ファイルをダブルクリックします。
   - Linux では、ファイルマネージャーの展開機能を使います。または `unzip process-thread-aggregator-main.zip` を実行します。

6. 展開した `process-thread-aggregator-main` フォルダーを開きます。フォルダー名の最後の部分は、GitHub のブランチ名によって変わる場合があります。
7. そのフォルダーでターミナルを開きます。

##### 方法 B: Git で clone する

PowerShell、コマンドプロンプト、またはターミナルで次のコマンドを実行します。

```text
git clone https://github.com/IshmamAbir/process-thread-aggregator.git
cd process-thread-aggregator
```

Git は `process-thread-aggregator` フォルダーを作り、リポジトリをダウンロードします。`cd` コマンドは、そのフォルダーへ移動するコマンドです。

#### ステップ 3: ソースファイルを確認する

Windows PowerShell では、次のコマンドを実行します。

```powershell
Get-ChildItem
```

Linux または macOS では、次のコマンドを実行します。

```bash
ls
```

次のファイルが表示されることを確認してください。

```text
go.mod
main.go
main_test.go
README.md
README.en.md
```

#### ステップ 4: プログラムを続けて実行する

Windows、Linux、macOS で同じコマンドを使えます。

```text
go run . -m 4 -n 2
```

このコマンドでは、次の設定を使います。

- `-m 4` は、各 producer process の中で generator thread を 4 個始めます。
- `-n 2` は、producer process を 2 個始めます。親の aggregator process もあるので、process は全部で 3 個です。
- `-run-for` を書かない場合は、自分で止めるまでプログラムが動き続けます。

ステップ 5 で出力を確認する間も、プログラムを動かしたままにしてください。

#### ステップ 5: 出力を確認する

最初の 1 秒が終わるまで待ちます。ターミナルには、1 行ごとに次のような JSON が表示されます。

```json
{"time":"1788223061","counts":{"0":8,"1":3,"2":6,"3":14,"4":7,"5":13,"6":15,"7":11,"8":11,"9":11}}
```

次のフィールドを確認してください。

- `time` は Unix timestamp の文字列です。
- `counts` には `0` から `9` までの 10 個のキーがあります。
- 各 count は 0 以上の整数です。

thread はランダムな数を作るので、実行するたびに count が変わります。JSON が 2 行以上表示されたら、`Ctrl+C` を 1 回押してプログラムを止めます。親 process は各 producer に停止コマンドを送り、終了を待ちます。2 秒たっても producer が終了しない場合は、強制終了します。

#### ステップ 6: 5 秒間だけ実行する

5 秒後にプログラムを止めたい場合は、`-run-for 5s` を付けます。

```text
go run . -m 2 -n 2 -run-for 5s
```

このコマンドは、2 個の producer process の中で generator thread を 2 個ずつ始めます。完了した 1 秒ごとの結果を表示し、5 秒後に producer process を終了して、ターミナルに戻ります。

#### ステップ 7: process と thread の数を変える

必要に合わせて `-m` と `-n` の値を変えられます。次の例では、producer process を 4 個、その中に generator thread を 8 個ずつ作ります。

```text
go run . -m 8 -n 4 -run-for 10s
```

どちらの値も 0 より大きくしてください。値を大きくすると、同時に行う処理と、1 秒間に作る数が増えます。

#### ステップ 8: テストを実行する

普通のテストを実行します。

```text
go test ./...
```

共有 map と goroutine の問題を調べるために、race detector も実行します。

```text
go test -race ./...
```

race detector には CGO と対応する C compiler が必要です。Windows では WSL を使うか、C compiler をインストールして `CGO_ENABLED=1` を設定してください。

#### ステップ 9: 実行ファイルを作る

実行ファイルを作らなくても、`go run .` でプログラムを確認できます。実行ファイルを作る場合は、次のコマンドを使います。

##### Linux と macOS

```text
go build -o concurrent-counter .
./concurrent-counter -m 2 -n 2 -run-for 5s
```

##### Windows PowerShell とコマンドプロンプト

```text
go build -o concurrent-counter.exe .
.\concurrent-counter.exe -m 2 -n 2 -run-for 5s
```

プログラムは、同じ実行ファイルから producer process を始めます。実行が終わるまで、実行ファイルを移動しないでください。

<a id="download-release"></a>

### 方法 2: Release をダウンロードする

Go をインストールせずに実行する場合は、[最新の Release](https://github.com/IshmamAbir/process-thread-aggregator/releases/latest) から作成済みの package をダウンロードしてください。

| OS | CPU | package の末尾 |
| --- | --- | --- |
| macOS | Apple silicon | `darwin_arm64.tar.gz` |
| macOS | Intel | `darwin_amd64.tar.gz` |
| Windows | Intel/AMD 64-bit | `windows_amd64.zip` |
| Windows | ARM 64-bit | `windows_arm64.zip` |
| Linux | Intel/AMD 64-bit | `linux_amd64.tar.gz` |
| Linux | ARM 64-bit | `linux_arm64.tar.gz` |

package を展開して、実行ファイルを起動します。

```text
# Linux または macOS
./concurrent-counter -m 2 -n 2 -run-for 5s

# Windows
.\concurrent-counter.exe -m 2 -n 2 -run-for 5s
```

Release の `checksums.txt` には、各 package の SHA-256 digest が入っています。

<a id="run-with-docker"></a>

### 方法 3: Docker で実行する

最初に、[ソースコードをダウンロード](#download-source)し、ダウンロードしたリポジトリのフォルダーでターミナルを開きます。リンク先では、GitHub の ZIP download と Git clone の両方を説明しています。

Docker をインストールして起動した後、リポジトリのフォルダーで image を作ります。

```text
docker build -t concurrent-counter .
```

プログラムを続けて実行します。停止するには `Ctrl+C` を押します。

```text
docker run --rm concurrent-counter -m 2 -n 2
```

プログラムを 5 秒間実行します。

```text
docker run --rm concurrent-counter -m 2 -n 2 -run-for 5s
```

image 名の後に書く option は、下のコマンドライン option と同じです。`--rm` は停止した container を削除します。

## コマンドラインのオプション

producer process を始めずに、使えるオプションを表示できます。

```text
go run . -h
```

Go の標準 flag parser では、`-h` と `-help` の両方を使えます。どちらも同じ説明を表示して終了します。

```text
go run . -help
```

作った実行ファイルでも同じように help flag を使えます。

```text
# Linux または macOS
./concurrent-counter -h

# Windows
.\concurrent-counter.exe -h
```

Docker でも同じように help flag を使えます。

```text
docker run --rm concurrent-counter -h
docker run --rm concurrent-counter -help
```

| オプション | 説明 | デフォルト |
| --- | --- | --- |
| `-h`, `-help` | 使えるオプションを表示し、プログラムを始めずに終了します。 | なし |
| `-m` | 各 producer process の generator thread の数です。0 より大きい値が必要です。 | `4` |
| `-n` | producer process の数です。aggregator process が 1 個増えるので、process の合計は `n + 1` 個です。0 より大きい値が必要です。 | `2` |
| `-run-for` | 実行する時間です。Go の duration 形式を使い、`500ms`、`5s`、`1m` などを書けます。`0` の場合は `Ctrl+C` を押すまで動きます。マイナスの値は使えません。 | `0` |

例:

```text
# 1 個の producer と 1 個の generator thread で 3 秒間実行する
go run . -m 1 -n 1 -run-for 3s

# 4 個の producer の中で generator thread を 8 個ずつ動かし、止めるまで実行する
go run . -m 8 -n 4
```

## 出力

プログラムは、完了した Unix time の 1 秒ごとに JSON を 1 行出力します。

```json
{"time":"1788223061","counts":{"0":8,"1":3,"2":6,"3":14,"4":7,"5":13,"6":15,"7":11,"8":11,"9":11}}
```

`time` は `[time, time + 1)` の時間を表します。`counts` には `0` から `9` までのキーがあり、count が 0 のキーも入ります。実行するタイミングによって作る値の数が変わるため、各 thread が 1 秒に必ず 100 個の値を作るとは限りません。

プログラムは、完全な 1 秒が終わってから最初の結果を表示します。開始前の短い時間と、終了中の短い時間のデータは使いません。そのため、1 秒より短い実行では JSON が表示されない場合があります。診断メッセージと入力エラーは標準エラー出力に書きます。標準出力には JSON だけを書くので、別のコマンドやファイルへ安全に送れます。

## チェックを実行する

リポジトリのフォルダーで次のコマンドを実行します。

```text
gofmt -l .
go vet ./...
go test ./...
go test -race ./...
go build -o concurrent-counter .
```

すべての Go ファイルが正しい形式なら、`gofmt -l .` は何も表示しません。ほかのコマンドは、問題がなければ終了 status `0` を返します。race detector には CGO と対応する C compiler が必要です。Windows では C compiler をインストールして `CGO_ENABLED=1` を設定するか、Linux または WSL で実行してください。

テストでは、counter の更新と秒の境界、共有 map への同時アクセス、producer 間の集計、JSON の作成、不正な入力、producer のエラー、終了時の処理、interrupt、複数 process での完全な実行を確認します。

## GitHub Release を公開する

名前が `v` で始まる tag を push すると、Release workflow が動きます。公開する commit に annotated semantic-version tag を作り、push してください。

```text
git tag -a v1.0.0 -m "v1.0.0"
git push origin v1.0.0
```

GitHub Actions は `go test ./...` を実行し、上の表にある 6 個の package と `checksums.txt` を作り、自動生成した release note と一緒に GitHub Release を公開します。Release の説明では、download link を Mac、Windows、Linux に分け、各 asset に同じ platform label を付けます。テストは Linux AMD64 で実行します。ほかの 5 個の package は Go で cross-compile しますが、それぞれの対象 OS では実行しません。

## 問題がある場合

- `go` が見つからない場合は、Go をインストールしてください。その後、`PATH` を更新するためにターミナルを開き直します。
- 何も表示されない場合は、`-run-for 2s` 以上を使ってください。プログラムは、完了した 1 秒の結果だけを表示します。
- flag の値でエラーが出た場合は、`-m` と `-n` に 0 より大きい値を使ってください。`-run-for` には 0 以上の Go duration を使います。
- `go test -race ./...` で CGO が必要だと表示された場合は、C compiler をインストールするか、Linux または WSL で実行してください。

## 設計と動作

このコマンドは aggregator process です。同じ実行ファイルから N 個の producer process を始めます。そのため、process は N 個の producer と 1 個の aggregator、合計 N + 1 個です。各 producer は M 個の generator goroutine を始めます。それぞれの goroutine は `runtime.LockOSThread` で OS thread に固定され、10 millisecond（0.01 秒）ごとに 0 から 9 までの整数を作ります。

各 producer は、1 秒ごとの `[10]uint64` count を普通の `map[int64][10]uint64` に保存します。producer ごとに 1 個の `sync.Mutex` を使い、時刻の取得、map の更新、snapshot の作成を安全に行います。aggregator は 1 個の event loop で自分のデータを管理するので、別の mutex は必要ありません。

親と各 child は、OS の anonymous pipe だけで通信します。親は child の標準入力へ、同じ時刻に開始する `start` コマンドと、終了する `stop` コマンドを送ります。child は標準出力へ、1 行ごとの JSON frame を送ります。親は、どの pipe がどの producer のものか知っています。同じ秒の N 個の frame を集計し、すべての producer から frame を受け取った後で、1 個の JSON を出力します。

すべての producer は、同じ秒の始まりに動き始めます。値を作らなかった秒も、10 個の count がすべて 0 の JSON を出力します。開始時の途中の秒と、終了時の途中の秒は使いません。そのため、1 秒より短い実行では何も表示されない場合があります。

標準出力には、1 行ごとの結果 JSON だけを書きます。help、入力エラー、child の診断メッセージ、実行エラーは標準エラー出力に書きます。そのため、標準出力を安全に別の場所へ送れます。

producer を開始できない場合、不正なデータや連続していないデータを受け取った場合、producer が早く終了した場合、終了処理でエラーが起きた場合は、親もすぐに終了します。親はほかの producer の control pipe を閉じ、最大 2 秒待ちます。その後も動いている child は強制終了します。producer をもう一度始めることはありません。
