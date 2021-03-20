const HtmlWebpackPlugin = require('html-webpack-plugin');
const CopyPlugin = require('copy-webpack-plugin');
const path = require('path');

module.exports = {
	entry: './index.ts',
	mode: 'development',
	output: {
		path: path.resolve(__dirname, '../dist'),
		filename: 'bundle.js',
	},
	module: {
		rules: [
			{ test: /\.svg$/, use: 'raw-loader' },
			{ test: /\.s[ac]ss$/, use: 'sass-loader' },
			{ test: /\.tsx?$/, use: 'ts-loader' },
		],
	},
	resolve: {
		extensions: ['.tsx', '.ts', '.js'],
	},
	plugins: [
		new HtmlWebpackPlugin({
			template: './index.html'
		}),
		new CopyPlugin({
			patterns: [
				{ from: "./assets/", to: "." },
				{ from: "./css/", to: "css" },
			],
		}),
	],
};
