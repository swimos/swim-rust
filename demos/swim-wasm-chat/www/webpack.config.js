const path = require('path');
const autoprefixer = require('autoprefixer');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const fs  = require('fs');
const lessToJs = require('less-vars-to-js');
const themeVariables = lessToJs(fs.readFileSync(path.join(__dirname, './ant-theme-vars.less'), 'utf8'));

module.exports = {
    devServer: {
        historyApiFallback: true,
    },
    entry: "./src/bootstrap.js",
    output: {
        path: path.resolve(__dirname, 'dist'),
        filename: 'bundle.js',
        chunkFilename: '[id].js',
    },
    resolve: {
        extensions: ['.js', '.jsx']
    },
    module: {
        rules: [
            {
                test: /\.js$/,
                loader: 'babel-loader',
                exclude: /node_modules/
            },
            {
                test: /\.css$/i,
                use: ['style-loader', 'css-loader'],
            },
            {
                test: /\.(png|jpe?g|gif)$/,
                loader: 'url-loader?limit=10000&name=img/[name].[extensions]'
            },{
                test: /\.less$/,
                use: [
                  {loader: "style-loader"},
                  {loader: "css-loader"},
                  {loader: "less-loader",
                    options: {
                      modifyVars: themeVariables,
                      javascriptEnabled: true
                    }
                  }
                ]
              }
        ]
    },
    plugins: [
        new HtmlWebpackPlugin({
            template: __dirname + '/src/index.html',
            filename: 'index.html',
            inject: 'body'
        })
    ]
};