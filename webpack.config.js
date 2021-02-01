const HtmlWebpackPlugin = require('html-webpack-plugin');
const CopyPlugin = require('copy-webpack-plugin');
const path = require('path');

module.exports = {
	entry: './client/js/entry.js',
	mode: 'development',
	output: {
		path: path.resolve(__dirname, './dist'),
		filename: 'bundle.js',
	},
	module: {
		rules: [
			{ test: /\.svg$/, use: 'raw-loader' },
			{ test: /\.s[ac]ss$/, use: 'sass-loader' },
		],
	},
	plugins: [
		new HtmlWebpackPlugin({
			template: './client/index.html'
		}),
		new CopyPlugin({
			patterns: [
				{ from: "./client/assets/", to: "." },
				{ from: "./client/css/", to: "css" },
			],
		}),
	],
};
